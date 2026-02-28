#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use node_spec_core::mcp::{
    AgentPolicy, McpAccessController, McpTool, ToolPermission, ValidationLimits,
};
use semver::Version;
use serde::Serialize;
use serde_json::{Value, json};
use starknet_node::replay::{ReplayCheck, ReplayEngine};
use starknet_node::{NodeConfig, StarknetNode, StarknetNodeBuilder};
use starknet_node_execution::{
    DualExecutionBackend, DualExecutionMetrics, ExecutionBackend, ExecutionError, ExecutionMode,
    MismatchPolicy,
};
use starknet_node_exex_btcfi::{
    BtcfiAnomaly, BtcfiExEx, StandardWrapper, StandardWrapperConfig, StandardWrapperMonitor,
    StrkBtcMonitor, StrkBtcMonitorConfig,
};
use starknet_node_mcp_server::{McpRequest, McpResponse};
use starknet_node_storage::{InMemoryStorage, StorageBackend};
use starknet_node_types::{
    BlockContext, BlockGasPrices, BuiltinStats, ContractAddress, ExecutionOutput, GasPricePerToken,
    InMemoryState, MutableState, SimulationResult, StarknetBlock, StarknetFelt, StarknetReceipt,
    StarknetStateDiff, StarknetTransaction, StateReader,
};
use tokio::net::TcpListener;
use tokio::time::interval;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_REFRESH_MS: u64 = 2_000;
const DEFAULT_REPLAY_WINDOW: u64 = 64;
const DEFAULT_MAX_REPLAY_PER_POLL: u64 = 16;
const DEMO_API_KEY: &str = "boss-demo-key";
const MAX_RECENT_ERRORS: usize = 16;
const MAX_RECENT_ANOMALIES: usize = 512;

const WBTC_CONTRACT: &str = "0x111";
const STRKBTC_SHIELDED_POOL: &str = "0x222";
const STRKBTC_MERKLE_ROOT_KEY: &str = "0x10";
const STRKBTC_COMMITMENT_COUNT_KEY: &str = "0x11";
const STRKBTC_NULLIFIER_COUNT_KEY: &str = "0x12";
const STRKBTC_NULLIFIER_PREFIX: &str = "0xdead";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DashboardModeKind {
    Demo,
    Real,
}

impl DashboardModeKind {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw.to_ascii_lowercase().as_str() {
            "demo" => Ok(Self::Demo),
            "real" => Ok(Self::Real),
            _ => Err(format!(
                "unsupported mode `{raw}`. expected `demo` or `real`"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Demo => "demo",
            Self::Real => "real",
        }
    }
}

#[derive(Debug, Clone)]
struct DashboardConfig {
    mode: DashboardModeKind,
    bind_addr: String,
    refresh_ms: u64,
    rpc_url: Option<String>,
    replay_window: u64,
    max_replay_per_poll: u64,
}

#[derive(Debug, Clone)]
struct RealBtcfiConfig {
    wrappers: Vec<StandardWrapperConfig>,
    strkbtc: StrkBtcMonitorConfig,
    max_retained_anomalies: usize,
}

#[derive(Clone)]
struct AppState {
    source: DashboardSource,
    refresh_ms: u64,
}

#[derive(Clone)]
enum DashboardSource {
    Demo { runtime: Arc<Mutex<DemoRuntime>> },
    Real { runtime: Arc<RealRuntime> },
}

struct DemoExecution;

impl ExecutionBackend for DemoExecution {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        _state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        Ok(ExecutionOutput {
            receipts: block
                .transactions
                .iter()
                .map(|tx| StarknetReceipt {
                    tx_hash: tx.hash.clone(),
                    execution_status: true,
                    events: 0,
                    gas_consumed: 1,
                })
                .collect(),
            state_diff: StarknetStateDiff::default(),
            builtin_stats: BuiltinStats::default(),
            execution_time: Duration::from_millis(1),
        })
    }

    fn simulate_tx(
        &self,
        tx: &StarknetTransaction,
        _state: &dyn StateReader,
        _block_context: &BlockContext,
    ) -> Result<SimulationResult, ExecutionError> {
        Ok(SimulationResult {
            receipt: StarknetReceipt {
                tx_hash: tx.hash.clone(),
                execution_status: true,
                events: 0,
                gas_consumed: 1,
            },
            estimated_fee: 1,
        })
    }
}

struct DemoRuntime {
    node: StarknetNode<InMemoryStorage, DualExecutionBackend>,
    execution_state: InMemoryState,
    btcfi: Mutex<BtcfiExEx>,
    next_block: u64,
    commitment_count: u64,
    nullifier_count: u64,
    merkle_root: u64,
    last_nullifier_key: Option<String>,
    tick_successes: u64,
    tick_failures: u64,
    replayed_tx_count: u64,
    replay_failures: u64,
    last_replay_error: Option<String>,
    last_error: Option<String>,
    recent_errors: VecDeque<String>,
}

impl DemoRuntime {
    fn new() -> Self {
        let node = build_dashboard_node();

        let wrapper = StandardWrapperMonitor::new(StandardWrapperConfig {
            wrapper: StandardWrapper::Wbtc,
            token_contract: ContractAddress::from(WBTC_CONTRACT),
            total_supply_key: "0x1".to_string(),
            expected_supply_sats: 1_000_000,
            allowed_deviation_bps: 50,
        });

        let strkbtc = StrkBtcMonitor::new(StrkBtcMonitorConfig {
            shielded_pool_contract: ContractAddress::from(STRKBTC_SHIELDED_POOL),
            merkle_root_key: STRKBTC_MERKLE_ROOT_KEY.to_string(),
            commitment_count_key: STRKBTC_COMMITMENT_COUNT_KEY.to_string(),
            nullifier_count_key: STRKBTC_NULLIFIER_COUNT_KEY.to_string(),
            nullifier_key_prefix: STRKBTC_NULLIFIER_PREFIX.to_string(),
            commitment_flood_threshold: 8,
            unshield_cluster_threshold: 4,
            unshield_cluster_window_blocks: 3,
            light_client_max_lag_blocks: 6,
            bridge_timeout_blocks: 20,
            max_tracked_nullifiers: 10_000,
        });

        Self {
            node,
            execution_state: InMemoryState::default(),
            btcfi: Mutex::new(BtcfiExEx::new(vec![wrapper], strkbtc, 256)),
            next_block: 1,
            commitment_count: 0,
            nullifier_count: 0,
            merkle_root: 0x100,
            last_nullifier_key: None,
            tick_successes: 0,
            tick_failures: 0,
            replayed_tx_count: 0,
            replay_failures: 0,
            last_replay_error: None,
            last_error: None,
            recent_errors: VecDeque::new(),
        }
    }

    fn tick(&mut self) -> Result<(), String> {
        let block_number = self.next_block;
        let block = demo_block(block_number);
        let diff = self.build_state_diff(block_number);

        if let Err(error) = self
            .node
            .execution
            .execute_block(&block, &mut self.execution_state)
        {
            let message = format!("execute block failed: {error}");
            self.replay_failures = self.replay_failures.saturating_add(1);
            self.last_replay_error = Some(message.clone());
            self.record_tick_error(message.clone());
            return Err(message);
        }
        self.replayed_tx_count = self
            .replayed_tx_count
            .saturating_add(block.transactions.len() as u64);
        self.last_replay_error = None;
        if let Err(error) = self.node.storage.insert_block(block, diff.clone()) {
            let message = format!("insert block failed: {error}");
            self.record_tick_error(message.clone());
            return Err(message);
        }
        if let Ok(mut btcfi) = self.btcfi.lock() {
            let _ = btcfi.process_block(block_number, &diff);
        } else {
            self.record_tick_error("btcfi lock poisoned while processing block".to_string());
        }
        self.next_block = self.next_block.saturating_add(1);
        self.tick_successes = self.tick_successes.saturating_add(1);
        Ok(())
    }

    fn dual_metrics(&self) -> Result<DualExecutionMetrics, String> {
        self.node
            .execution
            .metrics()
            .map_err(|error| format!("dual execution metrics unavailable: {error}"))
    }

    fn record_tick_error(&mut self, message: String) {
        self.tick_failures = self.tick_failures.saturating_add(1);
        self.last_error = Some(message.clone());
        push_recent_error(&mut self.recent_errors, message);
    }

    fn build_state_diff(&mut self, block_number: u64) -> StarknetStateDiff {
        let mut storage_diffs: BTreeMap<
            ContractAddress,
            BTreeMap<String, starknet_node_types::StarknetFelt>,
        > = BTreeMap::new();

        let wbtc_supply = if block_number.is_multiple_of(7) {
            1_200_000_u64
        } else {
            1_000_000_u64 + (block_number % 2_000)
        };
        storage_diffs.insert(
            ContractAddress::from(WBTC_CONTRACT),
            BTreeMap::from([(
                "0x1".to_string(),
                starknet_node_types::StarknetFelt::from(wbtc_supply),
            )]),
        );

        let commitment_delta = if block_number.is_multiple_of(5) {
            15
        } else {
            1
        };
        self.commitment_count = self.commitment_count.saturating_add(commitment_delta);

        let unshield_delta = if block_number.is_multiple_of(4) { 3 } else { 1 };
        self.nullifier_count = self.nullifier_count.saturating_add(unshield_delta);

        if !block_number.is_multiple_of(5) {
            self.merkle_root = self.merkle_root.saturating_add(1);
        }

        let nullifier_key = if block_number.is_multiple_of(9) {
            self.last_nullifier_key
                .clone()
                .unwrap_or_else(|| format!("{STRKBTC_NULLIFIER_PREFIX}0001"))
        } else {
            let generated = format!("{STRKBTC_NULLIFIER_PREFIX}{block_number:04x}");
            self.last_nullifier_key = Some(generated.clone());
            generated
        };

        storage_diffs.insert(
            ContractAddress::from(STRKBTC_SHIELDED_POOL),
            BTreeMap::from([
                (
                    STRKBTC_MERKLE_ROOT_KEY.to_string(),
                    starknet_node_types::StarknetFelt::from(self.merkle_root),
                ),
                (
                    STRKBTC_COMMITMENT_COUNT_KEY.to_string(),
                    starknet_node_types::StarknetFelt::from(self.commitment_count),
                ),
                (
                    STRKBTC_NULLIFIER_COUNT_KEY.to_string(),
                    starknet_node_types::StarknetFelt::from(self.nullifier_count),
                ),
                (
                    nullifier_key,
                    starknet_node_types::StarknetFelt::from(1_u64),
                ),
            ]),
        );

        StarknetStateDiff {
            storage_diffs,
            nonces: BTreeMap::new(),
            declared_classes: Vec::new(),
        }
    }

    fn mcp_access_controller(&self) -> McpAccessController {
        let permissions = BTreeSet::from([
            ToolPermission::GetNodeStatus,
            ToolPermission::GetAnomalies,
            ToolPermission::QueryState,
        ]);
        McpAccessController::new([(
            "boss-demo".to_string(),
            AgentPolicy::new(DEMO_API_KEY, permissions, 5_000),
        )])
    }

    fn mcp_limits(&self) -> ValidationLimits {
        ValidationLimits {
            max_batch_size: 64,
            max_depth: 4,
            max_total_tools: 256,
        }
    }

    fn query_anomalies_via_mcp(&self, limit: u64) -> Result<Vec<BtcfiAnomaly>, String> {
        let server = self.node.new_mcp_server_with_anomalies(
            self.mcp_access_controller(),
            self.mcp_limits(),
            &self.btcfi,
        );
        let response = server
            .handle_request(McpRequest {
                api_key: DEMO_API_KEY.to_string(),
                tool: McpTool::GetAnomalies { limit },
                now_unix_seconds: unix_now(),
            })
            .map_err(|error| format!("mcp get anomalies failed: {error}"))?;
        match response.response {
            McpResponse::GetAnomalies { anomalies } => Ok(anomalies),
            other => Err(format!(
                "unexpected MCP response for anomalies query: {other:?}"
            )),
        }
    }
}

fn build_dashboard_node() -> StarknetNode<InMemoryStorage, DualExecutionBackend> {
    let storage = InMemoryStorage::new(InMemoryState::default());
    StarknetNodeBuilder::new(NodeConfig::default())
        .with_storage(storage)
        .with_execution(build_dashboard_execution_backend())
        .with_rpc(true)
        .with_mcp(true)
        .build()
}

#[derive(Clone)]
struct RealRpcClient {
    http: reqwest::Client,
    rpc_url: String,
}

#[derive(Debug, Clone, Serialize)]
struct RealSnapshot {
    chain_id: String,
    latest_block: u64,
    state_root: String,
    tx_count: u64,
    rpc_latency_ms: u64,
    captured_unix_seconds: u64,
}

#[derive(Debug, Clone)]
struct RealBlockReplay {
    external_block_number: u64,
    block_hash: String,
    parent_hash: String,
    sequencer_address: String,
    timestamp: u64,
    transaction_hashes: Vec<String>,
}

#[derive(Debug, Clone)]
struct RealFetch {
    snapshot: RealSnapshot,
    replay: RealBlockReplay,
    state_diff: StarknetStateDiff,
    parse_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeDiagnostics {
    success_count: u64,
    failure_count: u64,
    consecutive_failures: u64,
    commit_failure_count: u64,
    execution_failure_count: u64,
    replayed_tx_count: u64,
    replay_failures: u64,
    last_success_unix_seconds: Option<u64>,
    last_failure_unix_seconds: Option<u64>,
    last_processed_block: Option<u64>,
    last_local_block: Option<u64>,
    external_head_block: Option<u64>,
    last_external_replayed_block: Option<u64>,
    replay_lag_blocks: Option<u64>,
    reorg_events: u64,
    last_error: Option<String>,
    last_replay_error: Option<String>,
    recent_errors: Vec<String>,
    anomaly_monitor_enabled: bool,
    retained_anomaly_count: usize,
    dual_mismatches: u64,
    dual_fast_executions: u64,
    dual_canonical_executions: u64,
}

impl RuntimeDiagnostics {
    fn new() -> Self {
        Self {
            success_count: 0,
            failure_count: 0,
            consecutive_failures: 0,
            commit_failure_count: 0,
            execution_failure_count: 0,
            replayed_tx_count: 0,
            replay_failures: 0,
            last_success_unix_seconds: None,
            last_failure_unix_seconds: None,
            last_processed_block: None,
            last_local_block: None,
            external_head_block: None,
            last_external_replayed_block: None,
            replay_lag_blocks: None,
            reorg_events: 0,
            last_error: None,
            last_replay_error: None,
            recent_errors: Vec::new(),
            anomaly_monitor_enabled: false,
            retained_anomaly_count: 0,
            dual_mismatches: 0,
            dual_fast_executions: 0,
            dual_canonical_executions: 0,
        }
    }
}

struct RealRuntimeState {
    node: StarknetNode<InMemoryStorage, DualExecutionBackend>,
    execution_state: InMemoryState,
    snapshot: Option<RealSnapshot>,
    replay: ReplayEngine,
    diagnostics: RuntimeDiagnostics,
    recent_errors: VecDeque<String>,
}

#[derive(Clone)]
struct RealRuntime {
    client: Arc<RealRpcClient>,
    btcfi: Option<Arc<Mutex<BtcfiExEx>>>,
    state: Arc<Mutex<RealRuntimeState>>,
}

impl RealRuntime {
    fn new(
        client: RealRpcClient,
        btcfi_config: Option<RealBtcfiConfig>,
        replay_window: u64,
        max_replay_per_poll: u64,
    ) -> Self {
        let btcfi = btcfi_config.map(|cfg| {
            let wrappers = cfg
                .wrappers
                .into_iter()
                .map(StandardWrapperMonitor::new)
                .collect();
            Arc::new(Mutex::new(BtcfiExEx::new(
                wrappers,
                StrkBtcMonitor::new(cfg.strkbtc),
                cfg.max_retained_anomalies,
            )))
        });
        let mut diagnostics = RuntimeDiagnostics::new();
        diagnostics.anomaly_monitor_enabled = btcfi.is_some();
        Self {
            client: Arc::new(client),
            btcfi,
            state: Arc::new(Mutex::new(RealRuntimeState {
                node: build_dashboard_node(),
                execution_state: InMemoryState::default(),
                snapshot: None,
                replay: ReplayEngine::new(replay_window, max_replay_per_poll),
                diagnostics,
                recent_errors: VecDeque::new(),
            })),
        }
    }

    async fn poll_once(&self) -> Result<RealSnapshot, String> {
        let external_head_block = match self.client.fetch_latest_block_number().await {
            Ok(number) => number,
            Err(error) => {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| "real runtime lock poisoned".to_string())?;
                record_real_runtime_failure(&mut guard, error.clone());
                return Err(error);
            }
        };

        let replay_plan = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "real runtime lock poisoned".to_string())?;
            guard.diagnostics.external_head_block = Some(external_head_block);
            let plan = guard.replay.plan(external_head_block);
            guard.diagnostics.replay_lag_blocks =
                Some(guard.replay.replay_lag_blocks(external_head_block));
            plan
        };

        for external_block in replay_plan {
            let fetch = match self.client.fetch_block(external_block).await {
                Ok(fetch) => fetch,
                Err(error) => {
                    let mut guard = self
                        .state
                        .lock()
                        .map_err(|_| "real runtime lock poisoned".to_string())?;
                    let message =
                        format!("failed to fetch external block {external_block}: {error}");
                    record_real_runtime_failure(&mut guard, message.clone());
                    return Err(message);
                }
            };

            let mut guard = self
                .state
                .lock()
                .map_err(|_| "real runtime lock poisoned".to_string())?;
            guard.snapshot = Some(fetch.snapshot.clone());
            for warning in &fetch.parse_warnings {
                push_recent_error(&mut guard.recent_errors, format!("warning: {warning}"));
            }

            let local_block_number = match guard
                .replay
                .check_block(external_block, &fetch.replay.parent_hash)
            {
                ReplayCheck::Continue { local_block_number } => local_block_number,
                ReplayCheck::Reorg {
                    conflicting_external_block,
                    ..
                } => {
                    let message = format!(
                        "reorg detected at external block {conflicting_external_block}: parent {} did not match expected parent",
                        fetch.replay.parent_hash
                    );
                    guard.diagnostics.reorg_events = guard.replay.reorg_events();
                    guard.diagnostics.replay_failures =
                        guard.diagnostics.replay_failures.saturating_add(1);
                    guard.diagnostics.last_replay_error = Some(message.clone());
                    guard.diagnostics.last_error = Some(message.clone());
                    push_recent_error(&mut guard.recent_errors, message);
                    guard.node = build_dashboard_node();
                    guard.execution_state = InMemoryState::default();
                    guard.diagnostics.last_local_block = None;
                    guard.diagnostics.last_external_replayed_block = None;
                    guard.diagnostics.replay_lag_blocks =
                        Some(guard.replay.replay_lag_blocks(external_head_block));
                    guard.diagnostics.recent_errors = guard.recent_errors.iter().cloned().collect();
                    break;
                }
            };

            let local_block =
                ingest_block_from_fetch(local_block_number, &fetch).map_err(|error| {
                    let message = format!(
                        "failed to build replay block local={} external={external_block}: {error}",
                        local_block_number
                    );
                    guard.diagnostics.replay_failures =
                        guard.diagnostics.replay_failures.saturating_add(1);
                    guard.diagnostics.last_replay_error = Some(message.clone());
                    record_real_runtime_failure(&mut guard, message.clone());
                    message
                })?;

            let mut execution_state = std::mem::take(&mut guard.execution_state);
            let execution_result = guard
                .node
                .execution
                .execute_block(&local_block, &mut execution_state);
            guard.execution_state = execution_state;
            if let Err(error) = execution_result {
                let message = format!(
                    "failed to execute local replay block local={} external={external_block}: {error}",
                    local_block_number
                );
                guard.diagnostics.execution_failure_count =
                    guard.diagnostics.execution_failure_count.saturating_add(1);
                guard.diagnostics.replay_failures =
                    guard.diagnostics.replay_failures.saturating_add(1);
                guard.diagnostics.last_replay_error = Some(message.clone());
                record_real_runtime_failure(&mut guard, message.clone());
                return Err(message);
            }

            guard.diagnostics.replayed_tx_count = guard
                .diagnostics
                .replayed_tx_count
                .saturating_add(local_block.transactions.len() as u64);
            guard.diagnostics.last_replay_error = None;
            if let Err(error) = guard
                .node
                .storage
                .insert_block(local_block, fetch.state_diff.clone())
            {
                let message = format!(
                    "failed to commit local replay block local={} external={external_block}: {error}",
                    local_block_number
                );
                guard.diagnostics.commit_failure_count =
                    guard.diagnostics.commit_failure_count.saturating_add(1);
                record_real_runtime_failure(&mut guard, message.clone());
                return Err(message);
            }

            if let Some(btcfi) = &self.btcfi {
                match btcfi.lock() {
                    Ok(mut monitor) => {
                        let _ = monitor.process_block(external_block, &fetch.state_diff);
                        guard.diagnostics.retained_anomaly_count =
                            monitor.recent_anomalies(MAX_RECENT_ANOMALIES).len();
                    }
                    Err(_) => {
                        let message = "btcfi monitor lock poisoned".to_string();
                        guard.diagnostics.last_error = Some(message.clone());
                        push_recent_error(&mut guard.recent_errors, message);
                    }
                }
            }

            match guard.node.execution.metrics() {
                Ok(metrics) => {
                    guard.diagnostics.dual_mismatches = metrics.mismatches;
                    guard.diagnostics.dual_fast_executions = metrics.fast_executions;
                    guard.diagnostics.dual_canonical_executions = metrics.canonical_executions;
                }
                Err(error) => push_recent_error(
                    &mut guard.recent_errors,
                    format!("warning: failed to read dual execution metrics: {error}"),
                ),
            }

            guard.diagnostics.last_local_block = Some(local_block_number);
            guard.diagnostics.last_processed_block = Some(external_block);
            guard.diagnostics.last_external_replayed_block = Some(external_block);
            guard
                .replay
                .mark_committed(external_block, &fetch.replay.block_hash);
            guard.diagnostics.replay_lag_blocks =
                Some(guard.replay.replay_lag_blocks(external_head_block));
        }

        let mut guard = self
            .state
            .lock()
            .map_err(|_| "real runtime lock poisoned".to_string())?;
        guard.diagnostics.success_count = guard.diagnostics.success_count.saturating_add(1);
        guard.diagnostics.consecutive_failures = 0;
        guard.diagnostics.last_success_unix_seconds = Some(unix_now());
        guard.diagnostics.last_error = None;
        guard.diagnostics.recent_errors = guard.recent_errors.iter().cloned().collect();
        guard
            .snapshot
            .clone()
            .ok_or_else(|| "real runtime has no snapshot after poll".to_string())
    }

    fn snapshot(&self) -> Result<Option<RealSnapshot>, String> {
        let guard = self
            .state
            .lock()
            .map_err(|_| "real runtime lock poisoned".to_string())?;
        Ok(guard.snapshot.clone())
    }

    async fn snapshot_or_refresh(&self) -> Result<RealSnapshot, String> {
        if let Some(snapshot) = self.snapshot()? {
            return Ok(snapshot);
        }
        self.poll_once().await
    }

    fn diagnostics(&self) -> Result<RuntimeDiagnostics, String> {
        let guard = self
            .state
            .lock()
            .map_err(|_| "real runtime lock poisoned".to_string())?;
        Ok(guard.diagnostics.clone())
    }

    fn anomaly_source_name(&self) -> &'static str {
        if self.btcfi.is_some() {
            "mcp:get_anomalies(local)"
        } else {
            "unconfigured"
        }
    }

    fn mcp_access_controller(&self) -> McpAccessController {
        let permissions = BTreeSet::from([
            ToolPermission::GetNodeStatus,
            ToolPermission::GetAnomalies,
            ToolPermission::QueryState,
        ]);
        McpAccessController::new([(
            "boss-real".to_string(),
            AgentPolicy::new(DEMO_API_KEY, permissions, 5_000),
        )])
    }

    fn mcp_limits(&self) -> ValidationLimits {
        ValidationLimits {
            max_batch_size: 64,
            max_depth: 4,
            max_total_tools: 256,
        }
    }

    fn query_anomalies_via_mcp(&self, limit: u64) -> Result<Option<Vec<BtcfiAnomaly>>, String> {
        let Some(btcfi) = &self.btcfi else {
            return Ok(None);
        };
        let guard = self
            .state
            .lock()
            .map_err(|_| "real runtime lock poisoned".to_string())?;
        let server = guard.node.new_mcp_server_with_anomalies(
            self.mcp_access_controller(),
            self.mcp_limits(),
            btcfi.as_ref(),
        );
        let response = server
            .handle_request(McpRequest {
                api_key: DEMO_API_KEY.to_string(),
                tool: McpTool::GetAnomalies { limit },
                now_unix_seconds: unix_now(),
            })
            .map_err(|error| format!("mcp get anomalies failed: {error}"))?;
        match response.response {
            McpResponse::GetAnomalies { anomalies } => Ok(Some(anomalies)),
            other => Err(format!(
                "unexpected MCP response for anomalies query: {other:?}"
            )),
        }
    }

    fn retained_anomaly_count(&self) -> Result<usize, String> {
        if self.btcfi.is_none() {
            return Ok(0);
        }
        let diagnostics = self.diagnostics()?;
        Ok(diagnostics.retained_anomaly_count)
    }

    fn anomaly_strings(&self, limit: usize) -> Result<Vec<String>, String> {
        if let Some(anomalies) = self.query_anomalies_via_mcp(limit as u64)? {
            return Ok(anomalies
                .into_iter()
                .map(|anomaly| format!("{anomaly:?}"))
                .collect());
        }
        let diagnostics = self.diagnostics()?;
        let mut notes = vec![format!(
            "Real mode connected to {}. BTCFi monitor is not configured; set PASTIS_MONITOR_STRKBTC_SHIELDED_POOL to enable it.",
            self.client.rpc_url
        )];
        if let Some(last_error) = diagnostics.last_error {
            notes.push(format!("Latest RPC error: {last_error}"));
        }
        if diagnostics.consecutive_failures > 0 {
            notes.push(format!(
                "Consecutive RPC failures: {}",
                diagnostics.consecutive_failures
            ));
        }
        notes.truncate(limit);
        Ok(notes)
    }
}

fn build_dashboard_execution_backend() -> DualExecutionBackend {
    DualExecutionBackend::new(
        Some(Box::new(DemoExecution)),
        Box::new(DemoExecution),
        ExecutionMode::DualWithVerification {
            verification_depth: 32,
        },
        MismatchPolicy::WarnAndFallback,
    )
}

impl RealRpcClient {
    fn new(rpc_url: String) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .map_err(|error| format!("failed to build HTTP client: {error}"))?;
        Ok(Self { http, rpc_url })
    }

    async fn fetch_latest_block_number(&self) -> Result<u64, String> {
        let block_head = self.call("starknet_blockHashAndNumber", json!([])).await?;
        value_as_u64(block_head.get("block_number").unwrap_or(&Value::Null)).ok_or_else(|| {
            format!("starknet_blockHashAndNumber missing block_number: {block_head}")
        })
    }

    async fn fetch_chain_id(&self) -> Result<String, String> {
        let chain_id_raw = self.call("starknet_chainId", json!([])).await?;
        Ok(chain_id_raw
            .as_str()
            .map(std::string::ToString::to_string)
            .unwrap_or_else(|| chain_id_raw.to_string()))
    }

    async fn fetch_block(&self, block_number: u64) -> Result<RealFetch, String> {
        let started = Instant::now();
        let mut parse_warnings = Vec::new();
        let chain_id = self.fetch_chain_id().await?;

        let block_selector = json!([{ "block_number": block_number }]);
        let block_with_txs = self
            .call("starknet_getBlockWithTxs", block_selector.clone())
            .await?;
        let replay = parse_block_with_txs(&block_with_txs, &mut parse_warnings)?;
        if replay.external_block_number != block_number {
            parse_warnings.push(format!(
                "block number mismatch between request ({block_number}) and getBlockWithTxs ({})",
                replay.external_block_number
            ));
        }
        let tx_count_raw = self
            .call("starknet_getBlockTransactionCount", block_selector.clone())
            .await?;
        let tx_count_from_rpc = value_as_u64(&tx_count_raw)
            .ok_or_else(|| format!("invalid tx count payload: {tx_count_raw}"))?;
        let tx_count_from_replay = replay.transaction_hashes.len() as u64;
        if tx_count_from_rpc != tx_count_from_replay {
            parse_warnings.push(format!(
                "tx count mismatch between getBlockTransactionCount ({tx_count_from_rpc}) and \
                 getBlockWithTxs ({tx_count_from_replay})"
            ));
        }

        let state_update = self
            .call("starknet_getStateUpdate", block_selector.clone())
            .await?;
        let state_root = state_update
            .get("new_root")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        if let Some(block_root) = block_with_txs.get("new_root").and_then(Value::as_str)
            && block_root != state_root
        {
            parse_warnings.push(format!(
                "state root mismatch between getBlockWithTxs ({block_root}) and \
                 getStateUpdate ({state_root})"
            ));
        }
        let (state_diff, mut state_diff_warnings) = state_update_to_diff(&state_update)?;
        parse_warnings.append(&mut state_diff_warnings);

        Ok(RealFetch {
            snapshot: RealSnapshot {
                chain_id,
                latest_block: block_number,
                state_root,
                tx_count: tx_count_from_replay,
                rpc_latency_ms: saturating_u128_to_u64(started.elapsed().as_millis()),
                captured_unix_seconds: unix_now(),
            },
            replay,
            state_diff,
            parse_warnings,
        })
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let response = self
            .http
            .post(&self.rpc_url)
            .json(&request)
            .send()
            .await
            .map_err(|error| format!("RPC {method} request failed: {error}"))?;
        let http_status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|error| format!("RPC {method} invalid JSON response: {error}"))?;

        if !http_status.is_success() {
            return Err(format!(
                "RPC {method} returned HTTP {} with body {}",
                http_status, body
            ));
        }
        if let Some(error) = body.get("error") {
            return Err(format!("RPC {method} error payload: {error}"));
        }
        body.get("result")
            .cloned()
            .ok_or_else(|| format!("RPC {method} response missing `result`: {body}"))
    }
}

#[derive(Debug, Serialize)]
struct StatusPayload {
    mode: String,
    chain_id: String,
    latest_block: u64,
    external_head_block: Option<u64>,
    local_replayed_block: Option<u64>,
    replay_lag_blocks: Option<u64>,
    reorg_events: u64,
    state_root: String,
    tx_count: u64,
    rpc_latency_ms: u64,
    mcp_roundtrip_ok: bool,
    anomaly_source: String,
    recent_anomaly_count: usize,
    dual_mismatches: u64,
    dual_fast_executions: u64,
    dual_canonical_executions: u64,
    replayed_tx_count: u64,
    replay_failures: u64,
    data_age_seconds: Option<u64>,
    failure_count: u64,
    consecutive_failures: u64,
    last_error: Option<String>,
    refresh_ms: u64,
}

#[derive(Debug, Serialize)]
struct AnomaliesPayload {
    source: String,
    anomalies: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DebugPayload {
    mode: String,
    refresh_ms: u64,
    success_count: u64,
    failure_count: u64,
    commit_failure_count: u64,
    execution_failure_count: u64,
    consecutive_failures: u64,
    last_success_unix_seconds: Option<u64>,
    last_failure_unix_seconds: Option<u64>,
    last_processed_block: Option<u64>,
    last_local_block: Option<u64>,
    external_head_block: Option<u64>,
    last_external_replayed_block: Option<u64>,
    replay_lag_blocks: Option<u64>,
    reorg_events: u64,
    anomaly_monitor_enabled: bool,
    retained_anomaly_count: usize,
    dual_mismatches: u64,
    dual_fast_executions: u64,
    dual_canonical_executions: u64,
    replayed_tx_count: u64,
    replay_failures: u64,
    last_replay_error: Option<String>,
    last_error: Option<String>,
    recent_errors: Vec<String>,
    snapshot_available: bool,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let config = parse_dashboard_config()?;
    let refresh_ms = config.refresh_ms.max(250);

    let source = match config.mode {
        DashboardModeKind::Demo => {
            let runtime = Arc::new(Mutex::new(DemoRuntime::new()));
            let simulation_runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_millis(refresh_ms));
                loop {
                    ticker.tick().await;
                    if let Ok(mut guard) = simulation_runtime.lock()
                        && let Err(error) = guard.tick()
                    {
                        eprintln!("warning: demo runtime tick failed: {error}");
                    }
                }
            });
            DashboardSource::Demo { runtime }
        }
        DashboardModeKind::Real => {
            let rpc_url = config.rpc_url.clone().ok_or_else(|| {
                "real mode requires --rpc-url <url> or STARKNET_RPC_URL".to_string()
            })?;
            let btcfi_config = parse_real_btcfi_config_from_env()?;
            let runtime = Arc::new(RealRuntime::new(
                RealRpcClient::new(rpc_url)?,
                btcfi_config,
                config.replay_window,
                config.max_replay_per_poll,
            ));
            if let Err(error) = runtime.poll_once().await {
                eprintln!("warning: initial real runtime poll failed: {error}");
            }
            let poll_runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_millis(refresh_ms));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    if let Err(error) = poll_runtime.poll_once().await {
                        eprintln!("warning: real runtime poll failed: {error}");
                    }
                }
            });
            DashboardSource::Real { runtime }
        }
    };

    let anomaly_source = match &source {
        DashboardSource::Demo { .. } => "mcp:get_anomalies".to_string(),
        DashboardSource::Real { runtime } => runtime.anomaly_source_name().to_string(),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/anomalies", get(anomalies))
        .route("/api/debug", get(debug))
        .with_state(AppState { source, refresh_ms });

    let listener = TcpListener::bind(&config.bind_addr)
        .await
        .map_err(|error| format!("failed to bind {}: {error}", config.bind_addr))?;
    println!("dashboard: http://{}/", config.bind_addr);
    println!("mode: {}", config.mode.as_str());
    if matches!(config.mode, DashboardModeKind::Demo) {
        println!("demo mcp api key: {DEMO_API_KEY}");
    }
    if let Some(rpc_url) = &config.rpc_url {
        println!("rpc url: {rpc_url}");
    }
    println!("anomaly source: {anomaly_source}");
    axum::serve(listener, app)
        .await
        .map_err(|error| format!("dashboard server failed: {error}"))?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn status(
    State(state): State<AppState>,
) -> Result<Json<StatusPayload>, (StatusCode, String)> {
    let payload = match &state.source {
        DashboardSource::Demo { runtime } => demo_status(runtime, state.refresh_ms),
        DashboardSource::Real { runtime } => real_status(runtime, state.refresh_ms).await,
    };
    payload
        .map(Json)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))
}

async fn anomalies(
    State(state): State<AppState>,
) -> Result<Json<AnomaliesPayload>, (StatusCode, String)> {
    let payload = match &state.source {
        DashboardSource::Demo { runtime } => demo_anomalies(runtime),
        DashboardSource::Real { runtime } => real_anomalies(runtime).await,
    };
    payload
        .map(Json)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))
}

async fn debug(State(state): State<AppState>) -> Result<Json<DebugPayload>, (StatusCode, String)> {
    let payload = match &state.source {
        DashboardSource::Demo { runtime } => demo_debug(runtime, state.refresh_ms),
        DashboardSource::Real { runtime } => real_debug(runtime, state.refresh_ms),
    };
    payload
        .map(Json)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))
}

fn demo_status(
    runtime: &Arc<Mutex<DemoRuntime>>,
    refresh_ms: u64,
) -> Result<StatusPayload, String> {
    let started = Instant::now();
    let guard = runtime
        .lock()
        .map_err(|_| "runtime lock poisoned".to_string())?;
    let latest_block = guard
        .node
        .storage
        .latest_block_number()
        .map_err(|error| format!("latest block read failed: {error}"))?;
    let state_root = guard
        .node
        .storage
        .current_state_root()
        .map_err(|error| format!("state root read failed: {error}"))?;
    let metrics = guard.dual_metrics()?;
    let (anomalies, mcp_roundtrip_ok) = nonfatal_anomaly_query(
        guard.query_anomalies_via_mcp(25),
        "demo status anomaly query",
    );
    Ok(StatusPayload {
        mode: DashboardModeKind::Demo.as_str().to_string(),
        chain_id: guard.node.config.chain_id.as_str().to_string(),
        latest_block,
        external_head_block: Some(latest_block),
        local_replayed_block: Some(latest_block),
        replay_lag_blocks: Some(0),
        reorg_events: 0,
        state_root,
        tx_count: 1,
        rpc_latency_ms: saturating_u128_to_u64(started.elapsed().as_millis()),
        mcp_roundtrip_ok,
        anomaly_source: "mcp:get_anomalies".to_string(),
        recent_anomaly_count: anomalies.len(),
        dual_mismatches: metrics.mismatches,
        dual_fast_executions: metrics.fast_executions,
        dual_canonical_executions: metrics.canonical_executions,
        replayed_tx_count: guard.replayed_tx_count,
        replay_failures: guard.replay_failures,
        data_age_seconds: Some(0),
        failure_count: guard.tick_failures,
        consecutive_failures: 0,
        last_error: guard.last_error.clone(),
        refresh_ms,
    })
}

fn demo_anomalies(runtime: &Arc<Mutex<DemoRuntime>>) -> Result<AnomaliesPayload, String> {
    let guard = runtime
        .lock()
        .map_err(|_| "runtime lock poisoned".to_string())?;
    let anomalies = guard.query_anomalies_via_mcp(25)?;
    Ok(AnomaliesPayload {
        source: "mcp:get_anomalies".to_string(),
        anomalies: anomalies
            .into_iter()
            .map(|anomaly| format!("{anomaly:?}"))
            .collect(),
    })
}

fn demo_debug(runtime: &Arc<Mutex<DemoRuntime>>, refresh_ms: u64) -> Result<DebugPayload, String> {
    let guard = runtime
        .lock()
        .map_err(|_| "runtime lock poisoned".to_string())?;
    let metrics = guard.dual_metrics()?;
    Ok(DebugPayload {
        mode: DashboardModeKind::Demo.as_str().to_string(),
        refresh_ms,
        success_count: guard.tick_successes,
        failure_count: guard.tick_failures,
        commit_failure_count: 0,
        execution_failure_count: guard.tick_failures,
        consecutive_failures: 0,
        last_success_unix_seconds: None,
        last_failure_unix_seconds: None,
        last_processed_block: Some(guard.next_block.saturating_sub(1)),
        last_local_block: Some(guard.next_block.saturating_sub(1)),
        external_head_block: Some(guard.next_block.saturating_sub(1)),
        last_external_replayed_block: Some(guard.next_block.saturating_sub(1)),
        replay_lag_blocks: Some(0),
        reorg_events: 0,
        anomaly_monitor_enabled: true,
        retained_anomaly_count: guard
            .query_anomalies_via_mcp(MAX_RECENT_ANOMALIES as u64)?
            .len(),
        dual_mismatches: metrics.mismatches,
        dual_fast_executions: metrics.fast_executions,
        dual_canonical_executions: metrics.canonical_executions,
        replayed_tx_count: guard.replayed_tx_count,
        replay_failures: guard.replay_failures,
        last_replay_error: guard.last_replay_error.clone(),
        last_error: guard.last_error.clone(),
        recent_errors: guard.recent_errors.iter().cloned().collect(),
        snapshot_available: guard.tick_successes > 0,
    })
}

async fn real_status(runtime: &Arc<RealRuntime>, refresh_ms: u64) -> Result<StatusPayload, String> {
    let snapshot = runtime.snapshot_or_refresh().await?;
    let diagnostics = runtime.diagnostics()?;
    let recent_anomaly_count = runtime.retained_anomaly_count()?;
    let mcp_roundtrip_ok = nonfatal_anomaly_probe(
        runtime.query_anomalies_via_mcp(1),
        "real status anomaly query",
    );
    let now = unix_now();
    let data_age_seconds = now.checked_sub(snapshot.captured_unix_seconds);
    Ok(StatusPayload {
        mode: DashboardModeKind::Real.as_str().to_string(),
        chain_id: snapshot.chain_id,
        latest_block: snapshot.latest_block,
        external_head_block: diagnostics.external_head_block,
        local_replayed_block: diagnostics.last_local_block,
        replay_lag_blocks: diagnostics.replay_lag_blocks,
        reorg_events: diagnostics.reorg_events,
        state_root: snapshot.state_root,
        tx_count: snapshot.tx_count,
        rpc_latency_ms: snapshot.rpc_latency_ms,
        mcp_roundtrip_ok,
        anomaly_source: runtime.anomaly_source_name().to_string(),
        recent_anomaly_count,
        dual_mismatches: diagnostics.dual_mismatches,
        dual_fast_executions: diagnostics.dual_fast_executions,
        dual_canonical_executions: diagnostics.dual_canonical_executions,
        replayed_tx_count: diagnostics.replayed_tx_count,
        replay_failures: diagnostics.replay_failures,
        data_age_seconds,
        failure_count: diagnostics.failure_count,
        consecutive_failures: diagnostics.consecutive_failures,
        last_error: diagnostics.last_error,
        refresh_ms,
    })
}

async fn real_anomalies(runtime: &Arc<RealRuntime>) -> Result<AnomaliesPayload, String> {
    let anomalies = runtime.anomaly_strings(25)?;
    Ok(AnomaliesPayload {
        source: runtime.anomaly_source_name().to_string(),
        anomalies,
    })
}

fn real_debug(runtime: &Arc<RealRuntime>, refresh_ms: u64) -> Result<DebugPayload, String> {
    let diagnostics = runtime.diagnostics()?;
    Ok(DebugPayload {
        mode: DashboardModeKind::Real.as_str().to_string(),
        refresh_ms,
        success_count: diagnostics.success_count,
        failure_count: diagnostics.failure_count,
        commit_failure_count: diagnostics.commit_failure_count,
        execution_failure_count: diagnostics.execution_failure_count,
        consecutive_failures: diagnostics.consecutive_failures,
        last_success_unix_seconds: diagnostics.last_success_unix_seconds,
        last_failure_unix_seconds: diagnostics.last_failure_unix_seconds,
        last_processed_block: diagnostics.last_processed_block,
        last_local_block: diagnostics.last_local_block,
        external_head_block: diagnostics.external_head_block,
        last_external_replayed_block: diagnostics.last_external_replayed_block,
        replay_lag_blocks: diagnostics.replay_lag_blocks,
        reorg_events: diagnostics.reorg_events,
        anomaly_monitor_enabled: diagnostics.anomaly_monitor_enabled,
        retained_anomaly_count: diagnostics.retained_anomaly_count,
        dual_mismatches: diagnostics.dual_mismatches,
        dual_fast_executions: diagnostics.dual_fast_executions,
        dual_canonical_executions: diagnostics.dual_canonical_executions,
        replayed_tx_count: diagnostics.replayed_tx_count,
        replay_failures: diagnostics.replay_failures,
        last_replay_error: diagnostics.last_replay_error,
        last_error: diagnostics.last_error,
        recent_errors: diagnostics.recent_errors,
        snapshot_available: runtime.snapshot()?.is_some(),
    })
}

fn demo_block(number: u64) -> StarknetBlock {
    StarknetBlock {
        number,
        parent_hash: format!("0x{:x}", number.saturating_sub(1)),
        state_root: format!("0x{:x}", number.saturating_mul(17)),
        timestamp: 1_700_000_000 + number,
        sequencer_address: ContractAddress::from("0x1234"),
        gas_prices: BlockGasPrices {
            l1_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
            l1_data_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
            l2_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
        },
        protocol_version: Version::parse("0.14.2").expect("demo protocol version must be valid"),
        transactions: vec![StarknetTransaction::new(format!("0x{number:x}"))],
    }
}

fn ingest_block_from_fetch(local_number: u64, fetch: &RealFetch) -> Result<StarknetBlock, String> {
    let replay = &fetch.replay;
    let parent_hash = StarknetFelt::from_str(&replay.parent_hash)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!(
                "invalid replay parent hash `{}`: {error}",
                replay.parent_hash
            )
        })?;
    let state_root = StarknetFelt::from_str(&fetch.snapshot.state_root)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!(
                "invalid replay state root `{}`: {error}",
                fetch.snapshot.state_root
            )
        })?;
    let sequencer_address = StarknetFelt::from_str(&replay.sequencer_address)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!(
                "invalid replay sequencer address `{}`: {error}",
                replay.sequencer_address
            )
        })?;
    let transactions = replay
        .transaction_hashes
        .iter()
        .cloned()
        .map(StarknetTransaction::new)
        .collect();
    let block = StarknetBlock {
        number: local_number,
        parent_hash,
        state_root,
        timestamp: replay.timestamp,
        sequencer_address: ContractAddress::from(sequencer_address),
        gas_prices: BlockGasPrices {
            l1_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
            l1_data_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
            l2_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
        },
        protocol_version: Version::parse("0.14.2").expect("replay protocol version must be valid"),
        transactions,
    };
    block
        .validate()
        .map_err(|error| format!("invalid replay block {local_number}: {error}"))?;
    Ok(block)
}

fn parse_dashboard_config() -> Result<DashboardConfig, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    parse_dashboard_config_from(
        args,
        env::var("PASTIS_DASHBOARD_MODE").ok(),
        env::var("PASTIS_DASHBOARD_BIND").ok(),
        env::var("PASTIS_DASHBOARD_REFRESH_MS").ok(),
        env::var("STARKNET_RPC_URL").ok(),
        env::var("PASTIS_DASHBOARD_REPLAY_WINDOW").ok(),
        env::var("PASTIS_DASHBOARD_MAX_REPLAY_PER_POLL").ok(),
    )
}

fn parse_dashboard_config_from(
    args: Vec<String>,
    env_mode: Option<String>,
    env_bind: Option<String>,
    env_refresh: Option<String>,
    env_rpc: Option<String>,
    env_replay_window: Option<String>,
    env_max_replay_per_poll: Option<String>,
) -> Result<DashboardConfig, String> {
    let mut cli_mode: Option<DashboardModeKind> = None;
    let mut cli_bind: Option<String> = None;
    let mut cli_refresh_ms: Option<u64> = None;
    let mut cli_rpc_url: Option<String> = None;
    let mut cli_replay_window: Option<u64> = None;
    let mut cli_max_replay_per_poll: Option<u64> = None;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--mode requires `demo` or `real`".to_string())?;
                cli_mode = Some(DashboardModeKind::parse(&raw)?);
            }
            "--rpc-url" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--rpc-url requires a value".to_string())?;
                cli_rpc_url = Some(raw);
            }
            "--bind" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--bind requires a value".to_string())?;
                cli_bind = Some(raw);
            }
            "--refresh-ms" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--refresh-ms requires a value".to_string())?;
                cli_refresh_ms = Some(parse_refresh_ms(&raw)?);
            }
            "--replay-window" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--replay-window requires a value".to_string())?;
                cli_replay_window = Some(parse_positive_u64(&raw)?);
            }
            "--max-replay-per-poll" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--max-replay-per-poll requires a value".to_string())?;
                cli_max_replay_per_poll = Some(parse_positive_u64(&raw)?);
            }
            "--help" | "-h" => {
                return Err(help_text());
            }
            unknown => {
                return Err(format!("unknown flag `{unknown}`\n\n{}", help_text()));
            }
        }
    }

    let mode = match cli_mode {
        Some(value) => value,
        None => env_mode
            .as_deref()
            .map(DashboardModeKind::parse)
            .transpose()?
            .unwrap_or(DashboardModeKind::Demo),
    };
    let refresh_ms = match cli_refresh_ms {
        Some(value) => value,
        None => env_refresh
            .as_deref()
            .map(parse_refresh_ms)
            .transpose()?
            .unwrap_or(DEFAULT_REFRESH_MS),
    };
    let replay_window = match cli_replay_window {
        Some(value) => value,
        None => env_replay_window
            .as_deref()
            .map(parse_positive_u64)
            .transpose()?
            .unwrap_or(DEFAULT_REPLAY_WINDOW),
    };
    let max_replay_per_poll = match cli_max_replay_per_poll {
        Some(value) => value,
        None => env_max_replay_per_poll
            .as_deref()
            .map(parse_positive_u64)
            .transpose()?
            .unwrap_or(DEFAULT_MAX_REPLAY_PER_POLL),
    };
    let config = DashboardConfig {
        mode,
        bind_addr: cli_bind
            .or(env_bind)
            .unwrap_or_else(|| DEFAULT_BIND_ADDR.to_string()),
        refresh_ms,
        rpc_url: cli_rpc_url.or(env_rpc),
        replay_window,
        max_replay_per_poll,
    };

    if config.mode == DashboardModeKind::Real && config.rpc_url.is_none() {
        return Err(
            "real mode requires --rpc-url <url> or STARKNET_RPC_URL environment variable"
                .to_string(),
        );
    }
    Ok(config)
}

fn parse_refresh_ms(raw: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|error| format!("invalid refresh value `{raw}`: {error}"))
}

fn parse_positive_u64(raw: &str) -> Result<u64, String> {
    let parsed = raw
        .parse::<u64>()
        .map_err(|error| format!("invalid positive integer `{raw}`: {error}"))?;
    if parsed == 0 {
        return Err(format!("value must be > 0, got `{raw}`"));
    }
    Ok(parsed)
}

fn parse_real_btcfi_config_from_env() -> Result<Option<RealBtcfiConfig>, String> {
    let Some(strkbtc_pool) = env::var("PASTIS_MONITOR_STRKBTC_SHIELDED_POOL").ok() else {
        return Ok(None);
    };

    let mut wrappers = Vec::new();
    if let Ok(wbtc_contract) = env::var("PASTIS_MONITOR_WBTC_CONTRACT") {
        wrappers.push(StandardWrapperConfig {
            wrapper: StandardWrapper::Wbtc,
            token_contract: ContractAddress::from(wbtc_contract),
            total_supply_key: env::var("PASTIS_MONITOR_WBTC_TOTAL_SUPPLY_KEY")
                .unwrap_or_else(|_| "0x1".to_string()),
            expected_supply_sats: parse_env_u128(
                "PASTIS_MONITOR_WBTC_EXPECTED_SUPPLY_SATS",
                1_000_000,
            )?,
            allowed_deviation_bps: parse_env_u64("PASTIS_MONITOR_WBTC_ALLOWED_DEVIATION_BPS", 50)?,
        });
    }

    let strkbtc = StrkBtcMonitorConfig {
        shielded_pool_contract: ContractAddress::from(strkbtc_pool),
        merkle_root_key: env::var("PASTIS_MONITOR_STRKBTC_MERKLE_ROOT_KEY")
            .unwrap_or_else(|_| STRKBTC_MERKLE_ROOT_KEY.to_string()),
        commitment_count_key: env::var("PASTIS_MONITOR_STRKBTC_COMMITMENT_COUNT_KEY")
            .unwrap_or_else(|_| STRKBTC_COMMITMENT_COUNT_KEY.to_string()),
        nullifier_count_key: env::var("PASTIS_MONITOR_STRKBTC_NULLIFIER_COUNT_KEY")
            .unwrap_or_else(|_| STRKBTC_NULLIFIER_COUNT_KEY.to_string()),
        nullifier_key_prefix: env::var("PASTIS_MONITOR_STRKBTC_NULLIFIER_PREFIX")
            .unwrap_or_else(|_| STRKBTC_NULLIFIER_PREFIX.to_string()),
        commitment_flood_threshold: parse_env_u64(
            "PASTIS_MONITOR_STRKBTC_COMMITMENT_FLOOD_THRESHOLD",
            8,
        )?,
        unshield_cluster_threshold: parse_env_u64(
            "PASTIS_MONITOR_STRKBTC_UNSHIELD_CLUSTER_THRESHOLD",
            4,
        )?,
        unshield_cluster_window_blocks: parse_env_u64(
            "PASTIS_MONITOR_STRKBTC_UNSHIELD_CLUSTER_WINDOW_BLOCKS",
            3,
        )?,
        light_client_max_lag_blocks: parse_env_u64(
            "PASTIS_MONITOR_STRKBTC_LIGHT_CLIENT_MAX_LAG_BLOCKS",
            6,
        )?,
        bridge_timeout_blocks: parse_env_u64("PASTIS_MONITOR_STRKBTC_BRIDGE_TIMEOUT_BLOCKS", 20)?,
        max_tracked_nullifiers: parse_env_usize(
            "PASTIS_MONITOR_STRKBTC_MAX_TRACKED_NULLIFIERS",
            10_000,
        )?,
    };

    Ok(Some(RealBtcfiConfig {
        wrappers,
        strkbtc,
        max_retained_anomalies: parse_env_usize(
            "PASTIS_MONITOR_MAX_RETAINED_ANOMALIES",
            MAX_RECENT_ANOMALIES,
        )?,
    }))
}

fn parse_env_u64(name: &str, default: u64) -> Result<u64, String> {
    match env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .map_err(|error| format!("invalid {name} value `{raw}`: {error}")),
        Err(_) => Ok(default),
    }
}

fn parse_env_u128(name: &str, default: u128) -> Result<u128, String> {
    match env::var(name) {
        Ok(raw) => raw
            .parse::<u128>()
            .map_err(|error| format!("invalid {name} value `{raw}`: {error}")),
        Err(_) => Ok(default),
    }
}

fn parse_env_usize(name: &str, default: usize) -> Result<usize, String> {
    match env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .map_err(|error| format!("invalid {name} value `{raw}`: {error}")),
        Err(_) => Ok(default),
    }
}

fn parse_block_with_txs(
    block_with_txs: &Value,
    warnings: &mut Vec<String>,
) -> Result<RealBlockReplay, String> {
    let external_block_number = value_as_u64(
        block_with_txs
            .get("block_number")
            .ok_or_else(|| format!("getBlockWithTxs missing block_number: {block_with_txs}"))?,
    )
    .ok_or_else(|| format!("invalid getBlockWithTxs block_number: {block_with_txs}"))?;
    let block_hash_raw = block_with_txs
        .get("block_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("getBlockWithTxs missing block_hash: {block_with_txs}"))?;
    let block_hash = StarknetFelt::from_str(block_hash_raw)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!("invalid getBlockWithTxs block_hash `{block_hash_raw}`: {error}")
        })?;
    let parent_hash_raw = block_with_txs
        .get("parent_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("getBlockWithTxs missing parent_hash: {block_with_txs}"))?;
    let parent_hash = StarknetFelt::from_str(parent_hash_raw)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!("invalid getBlockWithTxs parent_hash `{parent_hash_raw}`: {error}")
        })?;
    let sequencer_raw = block_with_txs
        .get("sequencer_address")
        .and_then(Value::as_str)
        .unwrap_or("0x1");
    let sequencer_address = StarknetFelt::from_str(sequencer_raw)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!("invalid getBlockWithTxs sequencer_address `{sequencer_raw}`: {error}")
        })?;
    let timestamp = value_as_u64(
        block_with_txs
            .get("timestamp")
            .ok_or_else(|| format!("getBlockWithTxs missing timestamp: {block_with_txs}"))?,
    )
    .ok_or_else(|| format!("invalid getBlockWithTxs timestamp: {block_with_txs}"))?;
    let transactions = block_with_txs
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("getBlockWithTxs missing transactions array: {block_with_txs}"))?;
    let mut transaction_hashes = Vec::with_capacity(transactions.len());
    for (idx, tx) in transactions.iter().enumerate() {
        let tx_hash_raw = if let Some(raw) = tx.as_str() {
            raw
        } else {
            tx.get("transaction_hash")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    format!("transaction {idx} in getBlockWithTxs missing transaction_hash: {tx}")
                })?
        };
        let tx_hash = StarknetFelt::from_str(tx_hash_raw)
            .map(|felt| format!("{:#x}", felt))
            .map_err(|error| {
                format!("invalid transaction_hash `{tx_hash_raw}` at index {idx}: {error}")
            })?;
        transaction_hashes.push(tx_hash);
    }
    if let Some(status_raw) = block_with_txs.get("status").and_then(Value::as_str)
        && status_raw.eq_ignore_ascii_case("PENDING")
    {
        warnings.push(
            "getBlockWithTxs returned a pending block; replay semantics may change on reorg"
                .to_string(),
        );
    }
    Ok(RealBlockReplay {
        external_block_number,
        block_hash,
        parent_hash,
        sequencer_address,
        timestamp,
        transaction_hashes,
    })
}

fn state_update_to_diff(state_update: &Value) -> Result<(StarknetStateDiff, Vec<String>), String> {
    let mut diff = StarknetStateDiff::default();
    let mut warnings = Vec::new();
    let state_diff = state_update
        .get("state_diff")
        .ok_or_else(|| format!("state_update is missing state_diff: {state_update}"))?;

    if let Some(storage_diffs) = state_diff.get("storage_diffs") {
        parse_storage_diffs(storage_diffs, &mut diff, &mut warnings);
    }
    if let Some(nonces) = state_diff.get("nonces") {
        parse_nonces(nonces, &mut diff, &mut warnings);
    }
    if let Some(declared_classes) = state_diff.get("declared_classes") {
        parse_declared_classes(declared_classes, &mut diff, &mut warnings);
    }
    if let Some(deprecated_declared_classes) = state_diff.get("deprecated_declared_classes") {
        parse_deprecated_declared_classes(deprecated_declared_classes, &mut diff, &mut warnings);
    }

    diff.validate()
        .map_err(|error| format!("converted state diff is invalid: {error}"))?;
    Ok((diff, warnings))
}

fn parse_storage_diffs(raw: &Value, diff: &mut StarknetStateDiff, warnings: &mut Vec<String>) {
    match raw {
        Value::Array(items) => {
            for item in items {
                let Some(address_raw) = item
                    .get("address")
                    .or_else(|| item.get("contract_address"))
                    .and_then(Value::as_str)
                else {
                    warnings.push(format!("storage diff entry missing address: {item}"));
                    continue;
                };
                let Some(contract) = parse_contract_address(address_raw, warnings) else {
                    continue;
                };
                let Some(entries) = item
                    .get("storage_entries")
                    .or_else(|| item.get("entries"))
                    .and_then(Value::as_array)
                else {
                    warnings.push(format!(
                        "storage diff entry missing storage_entries array for {address_raw}"
                    ));
                    continue;
                };
                let writes = diff.storage_diffs.entry(contract).or_default();
                for entry in entries {
                    parse_storage_entry(entry, writes, warnings);
                }
            }
        }
        Value::Object(object) => {
            for (address_raw, entries) in object {
                let Some(contract) = parse_contract_address(address_raw, warnings) else {
                    continue;
                };
                let writes = diff.storage_diffs.entry(contract).or_default();
                match entries {
                    Value::Array(items) => {
                        for entry in items {
                            parse_storage_entry(entry, writes, warnings);
                        }
                    }
                    Value::Object(map_entries) => {
                        for (key_raw, value_raw) in map_entries {
                            parse_storage_kv(key_raw, value_raw, writes, warnings);
                        }
                    }
                    other => warnings.push(format!(
                        "unsupported storage_diffs payload for {address_raw}: {other}"
                    )),
                }
            }
        }
        other => warnings.push(format!("unsupported storage_diffs shape: {other}")),
    }
}

fn parse_storage_entry(
    entry: &Value,
    writes: &mut BTreeMap<String, StarknetFelt>,
    warnings: &mut Vec<String>,
) {
    let Some(key_raw) = entry.get("key").and_then(Value::as_str) else {
        warnings.push(format!("storage entry missing key: {entry}"));
        return;
    };
    let Some(value_raw) = entry.get("value") else {
        warnings.push(format!(
            "storage entry missing value for key {key_raw}: {entry}"
        ));
        return;
    };
    parse_storage_kv(key_raw, value_raw, writes, warnings);
}

fn parse_storage_kv(
    key_raw: &str,
    value_raw: &Value,
    writes: &mut BTreeMap<String, StarknetFelt>,
    warnings: &mut Vec<String>,
) {
    let Some(storage_key) = canonicalize_hex_felt(key_raw, "storage key", warnings) else {
        return;
    };
    let Some(value) = value_as_felt(value_raw, "storage value", warnings) else {
        return;
    };
    writes.insert(storage_key, value);
}

fn parse_nonces(raw: &Value, diff: &mut StarknetStateDiff, warnings: &mut Vec<String>) {
    match raw {
        Value::Array(items) => {
            for item in items {
                let Some(address_raw) = item
                    .get("contract_address")
                    .or_else(|| item.get("address"))
                    .and_then(Value::as_str)
                else {
                    warnings.push(format!("nonce entry missing contract_address: {item}"));
                    continue;
                };
                let Some(contract) = parse_contract_address(address_raw, warnings) else {
                    continue;
                };
                let Some(nonce_raw) = item.get("nonce") else {
                    warnings.push(format!(
                        "nonce entry missing nonce for {address_raw}: {item}"
                    ));
                    continue;
                };
                let Some(nonce) = value_as_felt(nonce_raw, "nonce", warnings) else {
                    continue;
                };
                diff.nonces.insert(contract, nonce);
            }
        }
        Value::Object(object) => {
            for (address_raw, nonce_raw) in object {
                let Some(contract) = parse_contract_address(address_raw, warnings) else {
                    continue;
                };
                let Some(nonce) = value_as_felt(nonce_raw, "nonce", warnings) else {
                    continue;
                };
                diff.nonces.insert(contract, nonce);
            }
        }
        other => warnings.push(format!("unsupported nonces shape: {other}")),
    }
}

fn parse_declared_classes(raw: &Value, diff: &mut StarknetStateDiff, warnings: &mut Vec<String>) {
    let Some(items) = raw.as_array() else {
        warnings.push(format!("unsupported declared_classes shape: {raw}"));
        return;
    };
    for item in items {
        let class_hash_raw = if let Some(hash) = item.as_str() {
            hash
        } else if let Some(hash) = item.get("class_hash").and_then(Value::as_str) {
            hash
        } else {
            warnings.push(format!("declared class entry missing class_hash: {item}"));
            continue;
        };
        if let Some(class_hash) = canonicalize_hex_felt(class_hash_raw, "class hash", warnings) {
            diff.declared_classes.push(class_hash.into());
        }
    }
}

fn parse_deprecated_declared_classes(
    raw: &Value,
    diff: &mut StarknetStateDiff,
    warnings: &mut Vec<String>,
) {
    let Some(items) = raw.as_array() else {
        warnings.push(format!(
            "unsupported deprecated_declared_classes shape: {raw}"
        ));
        return;
    };
    for item in items {
        let Some(class_hash_raw) = item.as_str() else {
            warnings.push(format!(
                "deprecated declared class must be string hash, got: {item}"
            ));
            continue;
        };
        if let Some(class_hash) = canonicalize_hex_felt(class_hash_raw, "class hash", warnings) {
            diff.declared_classes.push(class_hash.into());
        }
    }
}

fn parse_contract_address(raw: &str, warnings: &mut Vec<String>) -> Option<ContractAddress> {
    canonicalize_hex_felt(raw, "contract address", warnings).map(ContractAddress::from)
}

fn canonicalize_hex_felt(raw: &str, field: &str, warnings: &mut Vec<String>) -> Option<String> {
    match StarknetFelt::from_str(raw) {
        Ok(value) => Some(format!("{:#x}", value)),
        Err(error) => {
            warnings.push(format!("invalid {field} `{raw}`: {error}"));
            None
        }
    }
}

fn value_as_felt(raw: &Value, field: &str, warnings: &mut Vec<String>) -> Option<StarknetFelt> {
    match raw {
        Value::String(value) => match StarknetFelt::from_str(value) {
            Ok(felt) => Some(felt),
            Err(error) => {
                warnings.push(format!("invalid {field} `{value}`: {error}"));
                None
            }
        },
        Value::Number(number) => {
            if let Some(value) = number.as_u64() {
                Some(StarknetFelt::from(value))
            } else {
                warnings.push(format!("non-u64 numeric {field} is unsupported: {number}"));
                None
            }
        }
        other => {
            warnings.push(format!("unsupported {field} type: {other}"));
            None
        }
    }
}

fn help_text() -> String {
    "usage: demo-dashboard [--mode demo|real] [--rpc-url <url>] [--bind <addr>] [--refresh-ms <ms>] [--replay-window <blocks>] [--max-replay-per-poll <blocks>]
environment:
  PASTIS_DASHBOARD_MODE=demo|real
  STARKNET_RPC_URL=https://...
  PASTIS_DASHBOARD_BIND=127.0.0.1:8080
  PASTIS_DASHBOARD_REFRESH_MS=2000
  PASTIS_DASHBOARD_REPLAY_WINDOW=64
  PASTIS_DASHBOARD_MAX_REPLAY_PER_POLL=16
  PASTIS_MONITOR_STRKBTC_SHIELDED_POOL=0x...
  PASTIS_MONITOR_WBTC_CONTRACT=0x..."
        .to_string()
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(raw) => {
            if let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
                u64::from_str_radix(hex, 16).ok()
            } else {
                raw.parse::<u64>().ok()
            }
        }
        _ => None,
    }
}

fn saturating_u128_to_u64(value: u128) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

fn push_recent_error(errors: &mut VecDeque<String>, message: String) {
    if errors.len() >= MAX_RECENT_ERRORS {
        let _ = errors.pop_front();
    }
    errors.push_back(message);
}

fn record_real_runtime_failure(state: &mut RealRuntimeState, message: String) {
    state.diagnostics.failure_count = state.diagnostics.failure_count.saturating_add(1);
    state.diagnostics.consecutive_failures =
        state.diagnostics.consecutive_failures.saturating_add(1);
    state.diagnostics.last_failure_unix_seconds = Some(unix_now());
    state.diagnostics.last_error = Some(message.clone());
    push_recent_error(&mut state.recent_errors, message);
    state.diagnostics.recent_errors = state.recent_errors.iter().cloned().collect();
}

fn nonfatal_anomaly_query(
    result: Result<Vec<BtcfiAnomaly>, String>,
    context: &str,
) -> (Vec<BtcfiAnomaly>, bool) {
    match result {
        Ok(anomalies) => (anomalies, true),
        Err(error) => {
            eprintln!("warning: {context} failed: {error}");
            (Vec::new(), false)
        }
    }
}

fn nonfatal_anomaly_probe(
    result: Result<Option<Vec<BtcfiAnomaly>>, String>,
    context: &str,
) -> bool {
    match result {
        Ok(found) => found.is_some(),
        Err(error) => {
            eprintln!("warning: {context} failed: {error}");
            false
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Pastis Live Dashboard</title>
  <style>
    :root {
      --bg: #f8f7f2;
      --panel: #ffffff;
      --ink: #0f172a;
      --muted: #64748b;
      --accent: #0f766e;
      --warn: #b45309;
      --danger: #b91c1c;
      --border: #d6d3d1;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      --sans: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
    }
    body {
      margin: 0;
      color: var(--ink);
      font-family: var(--sans);
      background:
        radial-gradient(800px 320px at 5% 0%, #d9f99d 0%, transparent 60%),
        radial-gradient(900px 380px at 100% 0%, #bfdbfe 0%, transparent 64%),
        var(--bg);
    }
    .wrap {
      max-width: 1180px;
      margin: 0 auto;
      padding: 24px;
    }
    .hero {
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }
    .title {
      margin: 0;
      font-size: 34px;
      letter-spacing: 0.2px;
    }
    .subtitle {
      margin: 8px 0 0 0;
      color: var(--muted);
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--border);
      background: #fff;
      border-radius: 999px;
      padding: 8px 12px;
      font-family: var(--mono);
      font-size: 12px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      box-shadow: 0 4px 10px rgba(15, 23, 42, 0.05);
    }
    .card h3 {
      margin: 0 0 7px 0;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .value {
      font-size: 25px;
      line-height: 1.2;
      font-weight: 650;
    }
    .mono {
      font-family: var(--mono);
      font-size: 14px;
      line-height: 1.35;
      word-break: break-word;
    }
    .ok { color: var(--accent); }
    .warn { color: var(--warn); }
    .danger { color: var(--danger); }
    .stack {
      display: grid;
      gap: 12px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
    }
    .panel h2 {
      margin: 0 0 10px 0;
      font-size: 18px;
    }
    .spark {
      font-family: var(--mono);
      font-size: 18px;
      letter-spacing: 1px;
      color: #334155;
      margin-bottom: 8px;
    }
    .list {
      list-style: none;
      margin: 0;
      padding: 0;
      display: grid;
      gap: 8px;
      max-height: 360px;
      overflow: auto;
    }
    .item {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 10px 12px;
      font-family: var(--mono);
      font-size: 12px;
      background: #fafafa;
      word-break: break-word;
    }
    .meta {
      margin-top: 12px;
      font-family: var(--mono);
      font-size: 12px;
      color: var(--muted);
    }
    @media (max-width: 700px) {
      .hero {
        flex-direction: column;
        align-items: flex-start;
      }
      .title { font-size: 28px; }
      .value { font-size: 21px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <h1 class="title">Pastis Boss Dashboard</h1>
        <p class="subtitle">Live Starknet visibility with mode-aware data source and anomaly stream.</p>
      </div>
      <div class="badge">Mode: <strong id="modeBadge">-</strong></div>
    </div>

    <div class="grid">
      <div class="card"><h3>Latest Block</h3><div class="value" id="latestBlock">-</div></div>
      <div class="card"><h3>External Head</h3><div class="value" id="externalHead">-</div></div>
      <div class="card"><h3>Local Replayed</h3><div class="value" id="localReplayed">-</div></div>
      <div class="card"><h3>Replay Lag (blocks)</h3><div class="value" id="replayLag">-</div></div>
      <div class="card"><h3>Reorg Events</h3><div class="value warn" id="reorgEvents">-</div></div>
      <div class="card"><h3>Block Tx Count</h3><div class="value" id="txCount">-</div></div>
      <div class="card"><h3>RPC Latency (ms)</h3><div class="value" id="rpcLatency">-</div></div>
      <div class="card"><h3>Chain ID</h3><div class="value mono" id="chainId">-</div></div>
      <div class="card"><h3>State Root</h3><div class="value mono" id="stateRoot">-</div></div>
      <div class="card"><h3>MCP Roundtrip</h3><div class="value" id="mcpStatus">-</div></div>
      <div class="card"><h3>Anomaly Source</h3><div class="value mono" id="anomalySource">-</div></div>
      <div class="card"><h3>Recent Anomalies</h3><div class="value warn" id="anomalyCount">-</div></div>
      <div class="card"><h3>Dual Mismatches</h3><div class="value danger" id="dualMismatches">-</div></div>
      <div class="card"><h3>Dual Fast Execs</h3><div class="value" id="dualFast">-</div></div>
      <div class="card"><h3>Dual Canonical Execs</h3><div class="value" id="dualCanonical">-</div></div>
      <div class="card"><h3>Replayed Txs</h3><div class="value" id="replayedTxs">-</div></div>
      <div class="card"><h3>Replay Failures</h3><div class="value danger" id="replayFailures">-</div></div>
      <div class="card"><h3>Data Age (s)</h3><div class="value" id="dataAge">-</div></div>
      <div class="card"><h3>Failures</h3><div class="value" id="failureCount">-</div></div>
    </div>

    <div class="stack">
      <section class="panel">
        <h2>Block Progression Sparkline</h2>
        <div class="spark" id="blockSpark">-</div>
      </section>

      <section class="panel">
        <h2>Recent Anomalies</h2>
        <ul class="list" id="anomalyList"></ul>
        <div class="meta" id="meta"></div>
      </section>
    </div>
  </div>

  <script>
    const blockHistory = [];
    const sparkChars = ['.', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    function toSeverityClass(text) {
      const lower = text.toLowerCase();
      if (lower.includes('critical') || lower.includes('reuse') || lower.includes('divergence')) return 'danger';
      if (lower.includes('flood') || lower.includes('timeout') || lower.includes('cluster')) return 'warn';
      return '';
    }

    function renderSparkline() {
      if (blockHistory.length < 2) {
        document.getElementById('blockSpark').textContent = '-';
        return;
      }
      const deltas = [];
      for (let i = 1; i < blockHistory.length; i += 1) {
        deltas.push(Math.max(0, blockHistory[i] - blockHistory[i - 1]));
      }
      const maxDelta = Math.max(...deltas, 1);
      const line = deltas.map((delta) => {
        const ratio = delta / maxDelta;
        const idx = Math.max(0, Math.min(sparkChars.length - 1, Math.round(ratio * (sparkChars.length - 1))));
        return sparkChars[idx];
      }).join('');
      document.getElementById('blockSpark').textContent = line || '-';
    }

    async function load() {
      const started = performance.now();
      const [statusRes, anomaliesRes, debugRes] = await Promise.all([
        fetch('/api/status'),
        fetch('/api/anomalies'),
        fetch('/api/debug'),
      ]);
      if (!statusRes.ok || !anomaliesRes.ok || !debugRes.ok) {
        document.getElementById('meta').textContent = 'Failed to load dashboard data';
        return 2000;
      }

      const status = await statusRes.json();
      const anomalies = await anomaliesRes.json();
      const debug = await debugRes.json();

      blockHistory.push(status.latest_block);
      if (blockHistory.length > 24) {
        blockHistory.shift();
      }
      renderSparkline();

      document.getElementById('modeBadge').textContent = String(status.mode || '-').toUpperCase();
      document.getElementById('modeBadge').className = status.mode === 'real' ? 'ok' : 'warn';
      document.getElementById('latestBlock').textContent = status.latest_block;
      document.getElementById('externalHead').textContent =
        status.external_head_block === null ? 'n/a' : String(status.external_head_block);
      document.getElementById('localReplayed').textContent =
        status.local_replayed_block === null ? 'n/a' : String(status.local_replayed_block);
      document.getElementById('replayLag').textContent =
        status.replay_lag_blocks === null ? 'n/a' : String(status.replay_lag_blocks);
      document.getElementById('reorgEvents').textContent = String(status.reorg_events || 0);
      document.getElementById('txCount').textContent = status.tx_count;
      document.getElementById('rpcLatency').textContent = status.rpc_latency_ms;
      document.getElementById('chainId').textContent = status.chain_id;
      document.getElementById('stateRoot').textContent = status.state_root;
      document.getElementById('anomalySource').textContent = status.anomaly_source;
      document.getElementById('anomalyCount').textContent = status.recent_anomaly_count;
      document.getElementById('dualMismatches').textContent = status.dual_mismatches;
      document.getElementById('dualFast').textContent = status.dual_fast_executions;
      document.getElementById('dualCanonical').textContent = status.dual_canonical_executions;
      document.getElementById('replayedTxs').textContent = status.replayed_tx_count;
      document.getElementById('replayFailures').textContent = status.replay_failures;
      document.getElementById('dataAge').textContent =
        status.data_age_seconds === null ? 'n/a' : String(status.data_age_seconds);
      document.getElementById('failureCount').textContent =
        `${status.failure_count} (${status.consecutive_failures}c)`;

      const mcp = document.getElementById('mcpStatus');
      if (status.mcp_roundtrip_ok) {
        mcp.textContent = 'OK';
        mcp.className = 'value ok';
      } else {
        const unconfigured = status.mode === 'real' && status.anomaly_source === 'unconfigured';
        mcp.textContent = unconfigured ? 'N/A' : 'FAIL';
        mcp.className = 'value ' + (unconfigured ? 'warn' : 'danger');
      }

      const list = document.getElementById('anomalyList');
      list.innerHTML = '';
      if (!anomalies.anomalies.length) {
        const li = document.createElement('li');
        li.className = 'item';
        li.textContent = 'No anomalies currently.';
        list.appendChild(li);
      } else {
        for (const item of anomalies.anomalies) {
          const li = document.createElement('li');
          li.className = 'item ' + toSeverityClass(item);
          li.textContent = item;
          list.appendChild(li);
        }
      }

      const fetchMs = Math.round(performance.now() - started);
      const replayError = debug.last_replay_error ? ` | last_replay_error=${debug.last_replay_error}` : '';
      const lastError = debug.last_error ? ` | last_error=${debug.last_error}` : '';
      document.getElementById('meta').textContent =
        `Refreshed in ${fetchMs}ms | API refresh=${status.refresh_ms}ms | anomaly source=${anomalies.source}${replayError}${lastError}`;
      const configuredRefresh = Number(status.refresh_ms);
      return Number.isFinite(configuredRefresh) && configuredRefresh >= 250 ? configuredRefresh : 2000;
    }

    async function tick() {
      const nextMs = await load().catch(() => 2000);
      setTimeout(tick, nextMs);
    }
    void tick();
  </script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dashboard_config_requires_rpc_in_real_mode() {
        let err = parse_dashboard_config_from(
            vec!["--mode".to_string(), "real".to_string()],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect_err("real mode without rpc must fail");
        assert!(err.contains("real mode requires --rpc-url"));
    }

    #[test]
    fn parse_dashboard_config_allows_real_mode_with_env_rpc() {
        let config = parse_dashboard_config_from(
            vec!["--mode".to_string(), "real".to_string()],
            None,
            None,
            None,
            Some("https://rpc.example".to_string()),
            None,
            None,
        )
        .expect("config should parse");
        assert_eq!(config.mode, DashboardModeKind::Real);
        assert_eq!(config.rpc_url.as_deref(), Some("https://rpc.example"));
    }

    #[test]
    fn parse_dashboard_config_cli_overrides_env() {
        let config = parse_dashboard_config_from(
            vec![
                "--mode".to_string(),
                "real".to_string(),
                "--bind".to_string(),
                "0.0.0.0:9999".to_string(),
                "--refresh-ms".to_string(),
                "1500".to_string(),
                "--rpc-url".to_string(),
                "https://cli.rpc".to_string(),
                "--replay-window".to_string(),
                "96".to_string(),
                "--max-replay-per-poll".to_string(),
                "12".to_string(),
            ],
            Some("demo".to_string()),
            Some("127.0.0.1:8080".to_string()),
            Some("2000".to_string()),
            Some("https://env.rpc".to_string()),
            Some("72".to_string()),
            Some("8".to_string()),
        )
        .expect("config should parse");

        assert_eq!(config.mode, DashboardModeKind::Real);
        assert_eq!(config.bind_addr, "0.0.0.0:9999");
        assert_eq!(config.refresh_ms, 1500);
        assert_eq!(config.rpc_url.as_deref(), Some("https://cli.rpc"));
        assert_eq!(config.replay_window, 96);
        assert_eq!(config.max_replay_per_poll, 12);
    }

    #[test]
    fn parse_dashboard_config_cli_overrides_invalid_env_values() {
        let config = parse_dashboard_config_from(
            vec![
                "--mode".to_string(),
                "real".to_string(),
                "--rpc-url".to_string(),
                "https://cli.rpc".to_string(),
                "--refresh-ms".to_string(),
                "1700".to_string(),
                "--replay-window".to_string(),
                "48".to_string(),
                "--max-replay-per-poll".to_string(),
                "6".to_string(),
            ],
            Some("invalid-mode".to_string()),
            None,
            Some("invalid-refresh".to_string()),
            Some("https://env.rpc".to_string()),
            Some("invalid-window".to_string()),
            Some("invalid-max".to_string()),
        )
        .expect("cli flags should override malformed env values");

        assert_eq!(config.mode, DashboardModeKind::Real);
        assert_eq!(config.rpc_url.as_deref(), Some("https://cli.rpc"));
        assert_eq!(config.refresh_ms, 1700);
        assert_eq!(config.replay_window, 48);
        assert_eq!(config.max_replay_per_poll, 6);
    }

    #[test]
    fn parse_dashboard_config_invalid_env_fails_without_cli_override() {
        let error = parse_dashboard_config_from(
            Vec::new(),
            Some("invalid-mode".to_string()),
            None,
            None,
            None,
            None,
            None,
        )
        .expect_err("invalid env mode must fail when no cli override is present");
        assert!(error.contains("unsupported mode"));
    }

    #[test]
    fn replay_engine_bootstraps_window_and_tracks_lag() {
        let mut replay = ReplayEngine::new(4, 3);
        assert_eq!(replay.plan(10), vec![7, 8, 9]);
        assert_eq!(
            replay.check_block(7, "0x0"),
            ReplayCheck::Continue {
                local_block_number: 1
            }
        );
        replay.mark_committed(7, "0xa");
        assert_eq!(replay.replay_lag_blocks(10), 3);
        assert_eq!(replay.expected_parent_hash(), Some("0xa"));
    }

    #[test]
    fn replay_engine_reset_for_reorg_rewinds_window() {
        let mut replay = ReplayEngine::new(5, 2);
        let _ = replay.plan(20);
        assert_eq!(
            replay.check_block(16, "0x0"),
            ReplayCheck::Continue {
                local_block_number: 1
            }
        );
        replay.mark_committed(16, "0x16");
        assert_eq!(
            replay.check_block(17, "0xdead"),
            ReplayCheck::Reorg {
                conflicting_external_block: 17,
                restart_external_block: 13,
            }
        );
        assert_eq!(replay.plan(20), vec![13, 14]);
        assert_eq!(replay.next_local_block(), 1);
    }

    #[test]
    fn value_as_u64_parses_decimal_and_hex() {
        assert_eq!(value_as_u64(&Value::String("42".to_string())), Some(42));
        assert_eq!(value_as_u64(&Value::String("0x2a".to_string())), Some(42));
        assert_eq!(value_as_u64(&Value::String("0X2A".to_string())), Some(42));
        assert_eq!(
            value_as_u64(&Value::Number(serde_json::Number::from(7))),
            Some(7)
        );
        assert_eq!(
            value_as_u64(&Value::String("not-a-number".to_string())),
            None
        );
    }

    #[test]
    fn push_recent_error_caps_history() {
        let mut errors = VecDeque::new();
        for idx in 0..(MAX_RECENT_ERRORS + 4) {
            push_recent_error(&mut errors, format!("error-{idx}"));
        }

        assert_eq!(errors.len(), MAX_RECENT_ERRORS);
        assert_eq!(errors.front().map(String::as_str), Some("error-4"));
        assert_eq!(errors.back().map(String::as_str), Some("error-19"));
    }

    #[test]
    fn state_update_to_diff_parses_array_shape() {
        let update = json!({
            "state_diff": {
                "storage_diffs": [{
                    "address": "0x111",
                    "storage_entries": [
                        {"key": "0x1", "value": "0x2"},
                        {"key": "0x2", "value": "0x3"}
                    ]
                }],
                "nonces": [{
                    "contract_address": "0x111",
                    "nonce": "0x9"
                }],
                "declared_classes": [{"class_hash": "0xabc"}],
                "deprecated_declared_classes": ["0xdef"]
            }
        });

        let (diff, warnings) = state_update_to_diff(&update).expect("diff should parse");
        assert!(warnings.is_empty());
        assert_eq!(
            diff.storage_diffs
                .get(&ContractAddress::from("0x111"))
                .and_then(|writes| writes.get("0x1"))
                .copied(),
            Some(StarknetFelt::from(2_u64))
        );
        assert_eq!(
            diff.nonces.get(&ContractAddress::from("0x111")).copied(),
            Some(StarknetFelt::from(9_u64))
        );
        assert_eq!(diff.declared_classes.len(), 2);
    }

    #[test]
    fn state_update_to_diff_parses_object_shape_with_warnings() {
        let update = json!({
            "state_diff": {
                "storage_diffs": {
                    "0x222": {
                        "0x10": "0x20",
                        "bad-key": "0x30"
                    }
                },
                "nonces": {
                    "0x222": "0x5"
                },
                "declared_classes": ["0x123", {"class_hash": "bad-hash"}]
            }
        });

        let (diff, warnings) = state_update_to_diff(&update).expect("diff should parse");
        assert!(!warnings.is_empty());
        assert_eq!(
            diff.storage_diffs
                .get(&ContractAddress::from("0x222"))
                .and_then(|writes| writes.get("0x10"))
                .copied(),
            Some(StarknetFelt::from(0x20_u64))
        );
        assert_eq!(
            diff.nonces.get(&ContractAddress::from("0x222")).copied(),
            Some(StarknetFelt::from(5_u64))
        );
    }

    #[test]
    fn ingest_block_from_fetch_builds_valid_block_and_rejects_invalid_root() {
        let fetch = RealFetch {
            snapshot: RealSnapshot {
                chain_id: "SN_MAIN".to_string(),
                latest_block: 7_000_123,
                state_root: "0x123".to_string(),
                tx_count: 0,
                rpc_latency_ms: 0,
                captured_unix_seconds: 1_700_000_000,
            },
            replay: RealBlockReplay {
                external_block_number: 7_000_123,
                block_hash: "0x7000123".to_string(),
                parent_hash: format!("0x{:x}", 7_000_122_u64),
                sequencer_address: "0x1".to_string(),
                timestamp: 1_700_000_000,
                transaction_hashes: Vec::new(),
            },
            state_diff: StarknetStateDiff::default(),
            parse_warnings: Vec::new(),
        };
        let block = ingest_block_from_fetch(3, &fetch).expect("fetch block should be valid");
        assert_eq!(block.number, 3);
        assert_eq!(block.parent_hash, format!("0x{:x}", 7_000_122_u64));
        assert_eq!(block.state_root, "0x123");
        block.validate().expect("ingested block must be valid");

        let mut invalid_fetch = fetch;
        invalid_fetch.snapshot.state_root = "bad-root".to_string();
        let error = ingest_block_from_fetch(4, &invalid_fetch).expect_err("invalid root must fail");
        assert!(error.contains("invalid replay state root"));
    }

    #[test]
    fn parse_block_with_txs_parses_variants_and_flags_pending() {
        let mut warnings = Vec::new();
        let block = json!({
            "block_number": 44,
            "block_hash": "0x2c",
            "parent_hash": "0xabc",
            "sequencer_address": "0x123",
            "timestamp": 1_701_000_000u64,
            "status": "PENDING",
            "transactions": [
                "0x1",
                {"transaction_hash": "0x2"}
            ]
        });

        let replay = parse_block_with_txs(&block, &mut warnings).expect("block should parse");
        assert_eq!(replay.external_block_number, 44);
        assert_eq!(replay.block_hash, "0x2c");
        assert_eq!(replay.parent_hash, "0xabc");
        assert_eq!(replay.sequencer_address, "0x123");
        assert_eq!(replay.timestamp, 1_701_000_000);
        assert_eq!(replay.transaction_hashes, vec!["0x1", "0x2"]);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("pending block")),
            "expected pending warning, got {warnings:?}"
        );
    }

    #[test]
    fn parse_block_with_txs_rejects_missing_transaction_hash() {
        let mut warnings = Vec::new();
        let block = json!({
            "block_number": 44,
            "block_hash": "0x2c",
            "parent_hash": "0xabc",
            "sequencer_address": "0x123",
            "timestamp": 1_701_000_000u64,
            "transactions": [
                {"type": "INVOKE"}
            ]
        });

        let error =
            parse_block_with_txs(&block, &mut warnings).expect_err("missing hash must fail");
        assert!(error.contains("missing transaction_hash"));
    }

    #[test]
    fn ingest_block_from_fetch_uses_replay_transaction_hashes() {
        let fetch = RealFetch {
            snapshot: RealSnapshot {
                chain_id: "SN_MAIN".to_string(),
                latest_block: 99,
                state_root: "0x555".to_string(),
                tx_count: 2,
                rpc_latency_ms: 10,
                captured_unix_seconds: 1_700_000_000,
            },
            replay: RealBlockReplay {
                external_block_number: 99,
                block_hash: "0x99".to_string(),
                parent_hash: "0x777".to_string(),
                sequencer_address: "0x123".to_string(),
                timestamp: 1_701_000_000,
                transaction_hashes: vec!["0x10".to_string(), "0x20".to_string()],
            },
            state_diff: StarknetStateDiff::default(),
            parse_warnings: Vec::new(),
        };

        let block = ingest_block_from_fetch(8, &fetch).expect("fetch should convert to block");
        assert_eq!(block.number, 8);
        assert_eq!(block.parent_hash, "0x777");
        assert_eq!(block.state_root, "0x555");
        assert_eq!(
            block
                .transactions
                .iter()
                .map(|tx| tx.hash.as_ref().to_string())
                .collect::<Vec<_>>(),
            vec!["0x10".to_string(), "0x20".to_string()]
        );
    }

    #[test]
    fn demo_runtime_tick_updates_dual_execution_metrics() {
        let mut runtime = DemoRuntime::new();
        runtime.tick().expect("tick should succeed");

        let metrics = runtime
            .node
            .execution
            .metrics()
            .expect("dual metrics should be readable");
        assert_eq!(metrics.mismatches, 0);
        assert_eq!(metrics.fast_executions, 1);
        assert_eq!(metrics.canonical_executions, 1);
    }

    #[test]
    fn nonfatal_anomaly_query_degrades_to_empty_results_on_error() {
        let (anomalies, roundtrip_ok) =
            nonfatal_anomaly_query(Err("mcp unavailable".to_string()), "test anomaly query");
        assert!(!roundtrip_ok);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn nonfatal_anomaly_probe_degrades_to_false_on_error() {
        let roundtrip_ok =
            nonfatal_anomaly_probe(Err("mcp unavailable".to_string()), "test anomaly probe");
        assert!(!roundtrip_ok);
    }
}
