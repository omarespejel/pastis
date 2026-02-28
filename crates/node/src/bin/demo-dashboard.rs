#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::future::Future;
use std::pin::Pin;
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
use starknet_node::replay::{
    FileReplayCheckpointStore, ReplayPipeline, ReplayPipelineError, ReplayPipelineStep,
};
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
    BlockContext, BlockGasPrices, BuiltinStats, ClassHash, ContractAddress, ExecutionOutput,
    GasPricePerToken, InMemoryState, MutableState, SimulationResult, StarknetBlock, StarknetFelt,
    StarknetReceipt, StarknetStateDiff, StarknetTransaction, StateReader, TxHash,
};
use tokio::net::TcpListener;
use tokio::time::interval;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_REFRESH_MS: u64 = 2_000;
const DEFAULT_REPLAY_WINDOW: u64 = 64;
const DEFAULT_MAX_REPLAY_PER_POLL: u64 = 16;
const DEFAULT_REPLAY_CHECKPOINT_FILE: &str = ".pastis/replay-checkpoint.json";
const DEFAULT_RPC_MAX_RETRIES: u32 = 2;
const DEFAULT_RPC_RETRY_BACKOFF_MS: u64 = 250;
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
    replay_checkpoint_path: Option<String>,
}

#[derive(Default)]
struct DashboardEnvConfig {
    mode: Option<String>,
    bind: Option<String>,
    refresh: Option<String>,
    rpc: Option<String>,
    replay_window: Option<String>,
    max_replay_per_poll: Option<String>,
    replay_checkpoint: Option<String>,
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
            token_contract: ContractAddress::parse(WBTC_CONTRACT).expect("valid contract address"),
            total_supply_key: "0x1".to_string(),
            expected_supply_sats: 1_000_000,
            allowed_deviation_bps: 50,
        });

        let strkbtc = StrkBtcMonitor::new(StrkBtcMonitorConfig {
            shielded_pool_contract: ContractAddress::parse(STRKBTC_SHIELDED_POOL).expect("valid contract address"),
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
            ContractAddress::parse(WBTC_CONTRACT).expect("valid contract address"),
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
            ContractAddress::parse(STRKBTC_SHIELDED_POOL).expect("valid contract address"),
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

type RpcFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

trait RealRpcSource: Send + Sync {
    fn rpc_url(&self) -> &str;
    fn fetch_latest_block_number(&self) -> RpcFuture<'_, u64>;
    fn fetch_block(&self, block_number: u64) -> RpcFuture<'_, RealFetch>;
}

#[derive(Debug, Clone)]
struct RpcRetryConfig {
    max_retries: u32,
    base_backoff: Duration,
}

impl Default for RpcRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_RPC_MAX_RETRIES,
            base_backoff: Duration::from_millis(DEFAULT_RPC_RETRY_BACKOFF_MS),
        }
    }
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
    last_state_diff: Option<StateDiffSummary>,
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
            last_state_diff: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct StateDiffSummary {
    fingerprint: String,
    touched_contracts: u64,
    storage_writes: u64,
    nonce_updates: u64,
    declared_classes: u64,
}

struct RealRuntimeState {
    node: StarknetNode<InMemoryStorage, DualExecutionBackend>,
    execution_state: InMemoryState,
    snapshot: Option<RealSnapshot>,
    replay: ReplayPipeline,
    diagnostics: RuntimeDiagnostics,
    recent_errors: VecDeque<String>,
}

#[derive(Clone)]
struct RealRuntime {
    client: Arc<dyn RealRpcSource>,
    retry_config: RpcRetryConfig,
    btcfi: Option<Arc<Mutex<BtcfiExEx>>>,
    state: Arc<Mutex<RealRuntimeState>>,
}

impl RealRuntime {
    fn new(
        client: RealRpcClient,
        btcfi_config: Option<RealBtcfiConfig>,
        replay_window: u64,
        max_replay_per_poll: u64,
        replay_checkpoint_path: Option<String>,
    ) -> Result<Self, String> {
        Self::new_with_rpc_source(
            Arc::new(client),
            btcfi_config,
            replay_window,
            max_replay_per_poll,
            replay_checkpoint_path,
            RpcRetryConfig::default(),
        )
    }

    fn new_with_rpc_source(
        client: Arc<dyn RealRpcSource>,
        btcfi_config: Option<RealBtcfiConfig>,
        replay_window: u64,
        max_replay_per_poll: u64,
        replay_checkpoint_path: Option<String>,
        retry_config: RpcRetryConfig,
    ) -> Result<Self, String> {
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
        let mut replay = new_replay_pipeline(
            replay_window,
            max_replay_per_poll,
            replay_checkpoint_path.as_deref(),
        )
        .map_err(|error| format!("failed to initialize replay pipeline: {error}"))?;
        let node = build_dashboard_node();
        let storage_tip = node
            .storage
            .latest_block_number()
            .map_err(|error| format!("failed to read local storage tip: {error}"))?;
        let mut recent_errors = VecDeque::new();
        let expected_next_local = storage_tip.saturating_add(1);
        if replay.next_local_block() != expected_next_local {
            let warning = format!(
                "replay checkpoint local cursor {} mismatches local storage tip {}; resetting replay cursor",
                replay.next_local_block(),
                storage_tip
            );
            push_recent_error(&mut recent_errors, warning);
            if let Some(path) = replay_checkpoint_path.as_deref() {
                match std::fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(format!(
                            "failed to remove stale replay checkpoint at {path}: {error}"
                        ));
                    }
                }
                replay = new_replay_pipeline(replay_window, max_replay_per_poll, Some(path))
                    .map_err(|error| {
                        format!("failed to reinitialize replay pipeline after reset: {error}")
                    })?;
            } else {
                replay = ReplayPipeline::new(replay_window, max_replay_per_poll);
            }
        }
        diagnostics.recent_errors = recent_errors.iter().cloned().collect();

        Ok(Self {
            client,
            retry_config,
            btcfi,
            state: Arc::new(Mutex::new(RealRuntimeState {
                node,
                execution_state: InMemoryState::default(),
                snapshot: None,
                replay,
                diagnostics,
                recent_errors,
            })),
        })
    }

    async fn fetch_latest_block_number_with_retry(&self) -> Result<u64, String> {
        let mut attempt = 0_u32;
        loop {
            match self.client.fetch_latest_block_number().await {
                Ok(head) => return Ok(head),
                Err(error) => {
                    if attempt >= self.retry_config.max_retries {
                        return Err(format!(
                            "latest block RPC failed after {} attempts: {error}",
                            self.retry_config.max_retries.saturating_add(1)
                        ));
                    }
                    let backoff = self.retry_backoff(attempt);
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }

    async fn fetch_block_with_retry(&self, block_number: u64) -> Result<RealFetch, String> {
        let mut attempt = 0_u32;
        loop {
            match self.client.fetch_block(block_number).await {
                Ok(fetch) => return Ok(fetch),
                Err(error) => {
                    if attempt >= self.retry_config.max_retries {
                        return Err(format!(
                            "block {block_number} RPC failed after {} attempts: {error}",
                            self.retry_config.max_retries.saturating_add(1)
                        ));
                    }
                    let backoff = self.retry_backoff(attempt);
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }

    fn retry_backoff(&self, attempt: u32) -> Duration {
        if self.retry_config.base_backoff.is_zero() {
            return Duration::ZERO;
        }
        let factor = 1_u128 << attempt.min(20);
        let base_ms = self.retry_config.base_backoff.as_millis();
        let backoff_ms = base_ms.saturating_mul(factor).min(5_000);
        Duration::from_millis(backoff_ms as u64)
    }

    async fn poll_once(&self) -> Result<RealSnapshot, String> {
        let external_head_block = match self.fetch_latest_block_number_with_retry().await {
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
            let fetch = match self.fetch_block_with_retry(external_block).await {
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
                .evaluate_block(external_block, &fetch.replay.parent_hash)
            {
                Ok(ReplayPipelineStep::Continue { local_block_number }) => local_block_number,
                Ok(ReplayPipelineStep::ReorgRecoverable {
                    conflicting_external_block,
                    restart_external_block,
                    depth,
                }) => {
                    let message = format!(
                        "recoverable reorg detected at external block {conflicting_external_block} \
                         (depth={depth}, restart={restart_external_block}): observed parent {}",
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
                Err(error) => {
                    let message = format!(
                        "replay guardrail failure at external block {external_block}: {error}"
                    );
                    guard.diagnostics.replay_failures =
                        guard.diagnostics.replay_failures.saturating_add(1);
                    guard.diagnostics.last_replay_error = Some(message.clone());
                    record_real_runtime_failure(&mut guard, message.clone());
                    return Err(message);
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
            guard.diagnostics.last_state_diff = Some(summarize_state_diff(&fetch.state_diff));
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
            if let Err(error) = guard
                .replay
                .mark_committed(external_block, &fetch.replay.block_hash)
            {
                let message = format!(
                    "failed to checkpoint replay state after external block {external_block}: {error}"
                );
                guard.diagnostics.replay_failures =
                    guard.diagnostics.replay_failures.saturating_add(1);
                guard.diagnostics.last_replay_error = Some(message.clone());
                record_real_runtime_failure(&mut guard, message.clone());
                return Err(message);
            }
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
            redact_rpc_url(self.client.rpc_url())
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

fn new_replay_pipeline(
    replay_window: u64,
    max_replay_per_poll: u64,
    checkpoint_path: Option<&str>,
) -> Result<ReplayPipeline, ReplayPipelineError> {
    let pipeline = ReplayPipeline::new(replay_window, max_replay_per_poll);
    match checkpoint_path {
        Some(path) => {
            pipeline.with_checkpoint_store(Arc::new(FileReplayCheckpointStore::new(path)))
        }
        None => Ok(pipeline),
    }
}

impl RealRpcSource for RealRpcClient {
    fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    fn fetch_latest_block_number(&self) -> RpcFuture<'_, u64> {
        Box::pin(RealRpcClient::fetch_latest_block_number(self))
    }

    fn fetch_block(&self, block_number: u64) -> RpcFuture<'_, RealFetch> {
        Box::pin(RealRpcClient::fetch_block(self, block_number))
    }
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
    last_state_diff_fingerprint: Option<String>,
    last_state_diff_touched_contracts: Option<u64>,
    last_state_diff_storage_writes: Option<u64>,
    last_state_diff_nonce_updates: Option<u64>,
    last_state_diff_declared_classes: Option<u64>,
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
                config.replay_checkpoint_path.clone(),
            )?);
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
        println!("rpc url: {}", redact_rpc_url(rpc_url));
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

fn redact_rpc_url(raw: &str) -> String {
    match reqwest::Url::parse(raw) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("unknown-host");
            let port = url.port().map(|value| format!(":{value}")).unwrap_or_default();
            format!("{}://{}{port}", url.scheme(), host)
        }
        Err(_) => "<invalid-rpc-url>".to_string(),
    }
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
        last_state_diff_fingerprint: None,
        last_state_diff_touched_contracts: None,
        last_state_diff_storage_writes: None,
        last_state_diff_nonce_updates: None,
        last_state_diff_declared_classes: None,
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
        last_state_diff_fingerprint: diagnostics
            .last_state_diff
            .as_ref()
            .map(|summary| summary.fingerprint.clone()),
        last_state_diff_touched_contracts: diagnostics
            .last_state_diff
            .as_ref()
            .map(|summary| summary.touched_contracts),
        last_state_diff_storage_writes: diagnostics
            .last_state_diff
            .as_ref()
            .map(|summary| summary.storage_writes),
        last_state_diff_nonce_updates: diagnostics
            .last_state_diff
            .as_ref()
            .map(|summary| summary.nonce_updates),
        last_state_diff_declared_classes: diagnostics
            .last_state_diff
            .as_ref()
            .map(|summary| summary.declared_classes),
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
        sequencer_address: ContractAddress::parse("0x1234").expect("valid contract address"),
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
        transactions: vec![StarknetTransaction::new(
            TxHash::parse(format!("0x{number:x}")).expect("valid demo tx hash"),
        )],
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
        .map(|hash| {
            TxHash::parse(hash)
                .map(StarknetTransaction::new)
                .map_err(|error| format!("invalid replay tx hash: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let block = StarknetBlock {
        number: local_number,
        parent_hash,
        state_root,
        timestamp: replay.timestamp,
        sequencer_address: ContractAddress::parse(sequencer_address).expect("valid contract address"),
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
        DashboardEnvConfig {
            mode: env::var("PASTIS_DASHBOARD_MODE").ok(),
            bind: env::var("PASTIS_DASHBOARD_BIND").ok(),
            refresh: env::var("PASTIS_DASHBOARD_REFRESH_MS").ok(),
            rpc: env::var("STARKNET_RPC_URL").ok(),
            replay_window: env::var("PASTIS_DASHBOARD_REPLAY_WINDOW").ok(),
            max_replay_per_poll: env::var("PASTIS_DASHBOARD_MAX_REPLAY_PER_POLL").ok(),
            replay_checkpoint: env::var("PASTIS_DASHBOARD_REPLAY_CHECKPOINT").ok(),
        },
    )
}

fn parse_dashboard_config_from(
    args: Vec<String>,
    env_config: DashboardEnvConfig,
) -> Result<DashboardConfig, String> {
    let mut cli_mode: Option<DashboardModeKind> = None;
    let mut cli_bind: Option<String> = None;
    let mut cli_refresh_ms: Option<u64> = None;
    let mut cli_rpc_url: Option<String> = None;
    let mut cli_replay_window: Option<u64> = None;
    let mut cli_max_replay_per_poll: Option<u64> = None;
    let mut cli_replay_checkpoint: Option<String> = None;

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
            "--replay-checkpoint" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--replay-checkpoint requires a value".to_string())?;
                cli_replay_checkpoint = Some(raw);
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
        None => env_config
            .mode
            .as_deref()
            .map(DashboardModeKind::parse)
            .transpose()?
            .unwrap_or(DashboardModeKind::Demo),
    };
    let refresh_ms = match cli_refresh_ms {
        Some(value) => value,
        None => env_config
            .refresh
            .as_deref()
            .map(parse_refresh_ms)
            .transpose()?
            .unwrap_or(DEFAULT_REFRESH_MS),
    };
    let replay_window = match cli_replay_window {
        Some(value) => value,
        None => env_config
            .replay_window
            .as_deref()
            .map(parse_positive_u64)
            .transpose()?
            .unwrap_or(DEFAULT_REPLAY_WINDOW),
    };
    let max_replay_per_poll = match cli_max_replay_per_poll {
        Some(value) => value,
        None => env_config
            .max_replay_per_poll
            .as_deref()
            .map(parse_positive_u64)
            .transpose()?
            .unwrap_or(DEFAULT_MAX_REPLAY_PER_POLL),
    };
    let config = DashboardConfig {
        mode,
        bind_addr: cli_bind
            .or(env_config.bind)
            .unwrap_or_else(|| DEFAULT_BIND_ADDR.to_string()),
        refresh_ms,
        rpc_url: cli_rpc_url.or(env_config.rpc),
        replay_window,
        max_replay_per_poll,
        replay_checkpoint_path: cli_replay_checkpoint
            .or(env_config.replay_checkpoint)
            .or_else(|| {
                (mode == DashboardModeKind::Real)
                    .then(|| DEFAULT_REPLAY_CHECKPOINT_FILE.to_string())
            }),
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
            token_contract: ContractAddress::parse(wbtc_contract).expect("valid contract address"),
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
        shielded_pool_contract: ContractAddress::parse(strkbtc_pool).expect("valid contract address"),
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
            match ClassHash::parse(&class_hash) {
                Ok(parsed) => diff.declared_classes.push(parsed),
                Err(error) => warnings.push(format!(
                    "invalid canonical class hash `{class_hash}`: {error}"
                )),
            }
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
            match ClassHash::parse(&class_hash) {
                Ok(parsed) => diff.declared_classes.push(parsed),
                Err(error) => warnings.push(format!(
                    "invalid canonical class hash `{class_hash}`: {error}"
                )),
            }
        }
    }
}

fn parse_contract_address(raw: &str, warnings: &mut Vec<String>) -> Option<ContractAddress> {
    canonicalize_hex_felt(raw, "contract address", warnings).and_then(|value| {
        ContractAddress::parse(value).map_or_else(
            |error| {
                warnings.push(format!("invalid canonical contract address `{raw}`: {error}"));
                None
            },
            Some,
        )
    })
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
    "usage: demo-dashboard [--mode demo|real] [--rpc-url <url>] [--bind <addr>] [--refresh-ms <ms>] [--replay-window <blocks>] [--max-replay-per-poll <blocks>] [--replay-checkpoint <path>]
environment:
  PASTIS_DASHBOARD_MODE=demo|real
  STARKNET_RPC_URL=https://...
  PASTIS_DASHBOARD_BIND=127.0.0.1:8080
  PASTIS_DASHBOARD_REFRESH_MS=2000
  PASTIS_DASHBOARD_REPLAY_WINDOW=64
  PASTIS_DASHBOARD_MAX_REPLAY_PER_POLL=16
  PASTIS_DASHBOARD_REPLAY_CHECKPOINT=.pastis/replay-checkpoint.json
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

const FNV64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV64_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Clone, Copy)]
struct Fnv64Hasher {
    state: u64,
}

impl Fnv64Hasher {
    fn new() -> Self {
        Self {
            state: FNV64_OFFSET_BASIS,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(FNV64_PRIME);
        }
    }

    fn update_str(&mut self, value: &str) {
        self.update(value.as_bytes());
        // Separator to avoid accidental collisions when concatenating adjacent fields.
        self.update(&[0xff]);
    }

    fn finish_hex(self) -> String {
        format!("fnv64:{:016x}", self.state)
    }
}

fn summarize_state_diff(diff: &StarknetStateDiff) -> StateDiffSummary {
    let mut hasher = Fnv64Hasher::new();
    let mut storage_writes = 0_u64;

    for (contract, writes) in &diff.storage_diffs {
        hasher.update_str("contract");
        hasher.update_str(contract.as_ref());
        for (key, value) in writes {
            storage_writes = storage_writes.saturating_add(1);
            hasher.update_str("storage-key");
            hasher.update_str(key);
            hasher.update_str("storage-value");
            hasher.update_str(&format!("{:#x}", value));
        }
    }

    for (contract, nonce) in &diff.nonces {
        hasher.update_str("nonce-contract");
        hasher.update_str(contract.as_ref());
        hasher.update_str("nonce-value");
        hasher.update_str(&format!("{:#x}", nonce));
    }

    for class_hash in &diff.declared_classes {
        hasher.update_str("declared-class");
        hasher.update_str(class_hash.as_ref());
    }

    StateDiffSummary {
        fingerprint: hasher.finish_hex(),
        touched_contracts: diff.storage_diffs.len() as u64,
        storage_writes,
        nonce_updates: diff.nonces.len() as u64,
        declared_classes: diff.declared_classes.len() as u64,
    }
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
    use std::path::PathBuf;

    use super::*;
    use starknet_node_types::BlockId;

    type HeadResponse = (Duration, Result<u64, String>);
    type BlockResponse = (u64, Duration, Result<RealFetch, String>);

    fn load_rpc_fixture(name: &str) -> Value {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("replay")
            .join("rpc")
            .join(name);
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("failed to read RPC fixture {}: {error}", path.display());
        });
        serde_json::from_str(&raw).unwrap_or_else(|error| {
            panic!("failed to parse RPC fixture {}: {error}", path.display());
        })
    }

    fn fetch_from_rpc_fixtures(block_fixture: &str, state_update_fixture: &str) -> RealFetch {
        let block_with_txs = load_rpc_fixture(block_fixture);
        let state_update = load_rpc_fixture(state_update_fixture);

        let mut parse_warnings = Vec::new();
        let replay = parse_block_with_txs(&block_with_txs, &mut parse_warnings)
            .expect("block fixture must parse successfully");
        assert!(
            parse_warnings.is_empty(),
            "unexpected block parse warnings: {parse_warnings:?}"
        );

        let (state_diff, state_warnings) =
            state_update_to_diff(&state_update).expect("state update fixture must parse");
        assert!(
            state_warnings.is_empty(),
            "unexpected state update warnings: {state_warnings:?}"
        );

        RealFetch {
            snapshot: RealSnapshot {
                chain_id: "SN_MAIN".to_string(),
                latest_block: replay.external_block_number,
                state_root: state_update
                    .get("new_root")
                    .and_then(Value::as_str)
                    .expect("fixture must expose new_root")
                    .to_string(),
                tx_count: replay.transaction_hashes.len() as u64,
                rpc_latency_ms: 0,
                captured_unix_seconds: 0,
            },
            replay,
            state_diff,
            parse_warnings,
        }
    }

    fn apply_fetch_to_local_chain(
        node: &mut StarknetNode<InMemoryStorage, DualExecutionBackend>,
        execution_state: &mut InMemoryState,
        local_block_number: u64,
        fetch: &RealFetch,
    ) {
        let block = ingest_block_from_fetch(local_block_number, fetch)
            .expect("fixture block ingestion should succeed");
        node.execution
            .execute_block(&block, execution_state)
            .expect("fixture block execution should succeed");
        node.storage
            .insert_block(block, fetch.state_diff.clone())
            .expect("fixture block commit should succeed");
    }

    struct MockRpcSource {
        rpc_url: String,
        head_responses: Mutex<VecDeque<HeadResponse>>,
        block_responses: Mutex<VecDeque<BlockResponse>>,
    }

    impl MockRpcSource {
        fn new(
            rpc_url: &str,
            head_responses: Vec<Result<u64, String>>,
            block_responses: Vec<(u64, Result<RealFetch, String>)>,
        ) -> Self {
            Self::new_with_delays(
                rpc_url,
                head_responses
                    .into_iter()
                    .map(|response| (Duration::ZERO, response))
                    .collect(),
                block_responses
                    .into_iter()
                    .map(|(block, response)| (block, Duration::ZERO, response))
                    .collect(),
            )
        }

        fn new_with_delays(
            rpc_url: &str,
            head_responses: Vec<HeadResponse>,
            block_responses: Vec<BlockResponse>,
        ) -> Self {
            Self {
                rpc_url: rpc_url.to_string(),
                head_responses: Mutex::new(head_responses.into()),
                block_responses: Mutex::new(block_responses.into()),
            }
        }
    }

    impl RealRpcSource for MockRpcSource {
        fn rpc_url(&self) -> &str {
            &self.rpc_url
        }

        fn fetch_latest_block_number(&self) -> RpcFuture<'_, u64> {
            Box::pin(async move {
                let (delay, response) = self
                    .head_responses
                    .lock()
                    .map_err(|_| "mock head responses lock poisoned".to_string())?
                    .pop_front()
                    .unwrap_or_else(|| {
                        (Duration::ZERO, Err("no scripted head response".to_string()))
                    });
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                response
            })
        }

        fn fetch_block(&self, block_number: u64) -> RpcFuture<'_, RealFetch> {
            Box::pin(async move {
                let next = {
                    let mut guard = self
                        .block_responses
                        .lock()
                        .map_err(|_| "mock block responses lock poisoned".to_string())?;
                    guard.pop_front()
                };
                let Some((expected_block, delay, response)) = next else {
                    return Err(format!(
                        "no scripted block response for request block {block_number}"
                    ));
                };
                if expected_block != block_number {
                    return Err(format!(
                        "mock block request mismatch: expected {expected_block}, got {block_number}"
                    ));
                }
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                response
            })
        }
    }

    #[test]
    fn parse_dashboard_config_requires_rpc_in_real_mode() {
        let err = parse_dashboard_config_from(
            vec!["--mode".to_string(), "real".to_string()],
            DashboardEnvConfig::default(),
        )
        .expect_err("real mode without rpc must fail");
        assert!(err.contains("real mode requires --rpc-url"));
    }

    #[test]
    fn parse_dashboard_config_allows_real_mode_with_env_rpc() {
        let config = parse_dashboard_config_from(
            vec!["--mode".to_string(), "real".to_string()],
            DashboardEnvConfig {
                rpc: Some("https://rpc.example".to_string()),
                ..DashboardEnvConfig::default()
            },
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
            DashboardEnvConfig {
                mode: Some("demo".to_string()),
                bind: Some("127.0.0.1:8080".to_string()),
                refresh: Some("2000".to_string()),
                rpc: Some("https://env.rpc".to_string()),
                replay_window: Some("72".to_string()),
                max_replay_per_poll: Some("8".to_string()),
                ..DashboardEnvConfig::default()
            },
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
            DashboardEnvConfig {
                mode: Some("invalid-mode".to_string()),
                refresh: Some("invalid-refresh".to_string()),
                rpc: Some("https://env.rpc".to_string()),
                replay_window: Some("invalid-window".to_string()),
                max_replay_per_poll: Some("invalid-max".to_string()),
                ..DashboardEnvConfig::default()
            },
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
            DashboardEnvConfig {
                mode: Some("invalid-mode".to_string()),
                ..DashboardEnvConfig::default()
            },
        )
        .expect_err("invalid env mode must fail when no cli override is present");
        assert!(error.contains("unsupported mode"));
    }

    #[test]
    fn parse_dashboard_config_real_mode_defaults_checkpoint_path() {
        let config = parse_dashboard_config_from(
            vec![
                "--mode".to_string(),
                "real".to_string(),
                "--rpc-url".to_string(),
                "https://rpc.example".to_string(),
            ],
            DashboardEnvConfig::default(),
        )
        .expect("config should parse");
        assert_eq!(
            config.replay_checkpoint_path.as_deref(),
            Some(DEFAULT_REPLAY_CHECKPOINT_FILE)
        );
    }

    #[tokio::test]
    async fn real_runtime_retries_head_rpc_and_succeeds_without_recording_failure() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );

        let runtime = RealRuntime::new_with_rpc_source(
            Arc::new(MockRpcSource::new(
                "mock://rpc",
                vec![Err("timeout".to_string()), Ok(7_242_623)],
                vec![(7_242_623, Ok(fetch_623.clone()))],
            )),
            None,
            1,
            1,
            None,
            RpcRetryConfig {
                max_retries: 1,
                base_backoff: Duration::ZERO,
            },
        )
        .expect("runtime should initialize");

        let snapshot = runtime.poll_once().await.expect("poll should succeed");
        assert_eq!(snapshot.latest_block, 7_242_623);

        let diagnostics = runtime
            .diagnostics()
            .expect("diagnostics should be readable");
        assert_eq!(diagnostics.success_count, 1);
        assert_eq!(diagnostics.failure_count, 0);
        assert_eq!(diagnostics.consecutive_failures, 0);
        assert_eq!(diagnostics.last_local_block, Some(1));
        assert_eq!(
            diagnostics.replayed_tx_count,
            fetch_623.replay.transaction_hashes.len() as u64
        );
    }

    #[tokio::test]
    async fn real_runtime_reports_failure_after_retry_budget_exhausted() {
        let runtime = RealRuntime::new_with_rpc_source(
            Arc::new(MockRpcSource::new(
                "mock://rpc",
                vec![Err("timeout-1".to_string()), Err("timeout-2".to_string())],
                Vec::new(),
            )),
            None,
            1,
            1,
            None,
            RpcRetryConfig {
                max_retries: 1,
                base_backoff: Duration::ZERO,
            },
        )
        .expect("runtime should initialize");

        let error = runtime
            .poll_once()
            .await
            .expect_err("poll should fail after retries");
        assert!(
            error.contains("after 2 attempts"),
            "unexpected error: {error}"
        );

        let diagnostics = runtime
            .diagnostics()
            .expect("diagnostics should be readable");
        assert_eq!(diagnostics.success_count, 0);
        assert_eq!(diagnostics.failure_count, 1);
        assert_eq!(diagnostics.consecutive_failures, 1);
        assert!(
            diagnostics
                .last_error
                .as_deref()
                .is_some_and(|msg| msg.contains("after 2 attempts"))
        );
    }

    #[tokio::test]
    async fn real_runtime_resumes_after_transient_block_fetch_failure() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );

        let runtime = RealRuntime::new_with_rpc_source(
            Arc::new(MockRpcSource::new(
                "mock://rpc",
                vec![Ok(7_242_623), Ok(7_242_624)],
                vec![
                    (7_242_623, Ok(fetch_623.clone())),
                    (7_242_624, Err("429 rate limited".to_string())),
                    (7_242_624, Ok(fetch_624.clone())),
                ],
            )),
            None,
            1,
            1,
            None,
            RpcRetryConfig {
                max_retries: 1,
                base_backoff: Duration::ZERO,
            },
        )
        .expect("runtime should initialize");

        let first = runtime
            .poll_once()
            .await
            .expect("first poll should succeed");
        assert_eq!(first.latest_block, 7_242_623);

        let second = runtime
            .poll_once()
            .await
            .expect("second poll should succeed");
        assert_eq!(second.latest_block, 7_242_624);

        let diagnostics = runtime
            .diagnostics()
            .expect("diagnostics should be readable");
        assert_eq!(diagnostics.success_count, 2);
        assert_eq!(diagnostics.failure_count, 0);
        assert_eq!(diagnostics.consecutive_failures, 0);
        assert_eq!(diagnostics.last_local_block, Some(2));
        assert_eq!(diagnostics.last_external_replayed_block, Some(7_242_624));
        assert_eq!(
            diagnostics.replayed_tx_count,
            (fetch_623.replay.transaction_hashes.len() + fetch_624.replay.transaction_hashes.len())
                as u64
        );
    }

    #[tokio::test]
    async fn real_runtime_restart_reconciles_checkpoint_with_fresh_local_storage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let checkpoint_path = dir.path().join("replay-checkpoint.json");
        let checkpoint_path_str = checkpoint_path
            .to_str()
            .expect("checkpoint path should be valid utf8")
            .to_string();

        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );

        let first_boot = RealRuntime::new_with_rpc_source(
            Arc::new(MockRpcSource::new(
                "mock://rpc",
                vec![Ok(7_242_623)],
                vec![(7_242_623, Ok(fetch_623.clone()))],
            )),
            None,
            1,
            1,
            Some(checkpoint_path_str.clone()),
            RpcRetryConfig {
                max_retries: 0,
                base_backoff: Duration::ZERO,
            },
        )
        .expect("first runtime should initialize");
        first_boot
            .poll_once()
            .await
            .expect("first runtime poll should succeed");

        let second_boot = RealRuntime::new_with_rpc_source(
            Arc::new(MockRpcSource::new(
                "mock://rpc",
                vec![Ok(7_242_624)],
                vec![(7_242_624, Ok(fetch_624.clone()))],
            )),
            None,
            1,
            1,
            Some(checkpoint_path_str),
            RpcRetryConfig {
                max_retries: 0,
                base_backoff: Duration::ZERO,
            },
        )
        .expect("second runtime should initialize");
        second_boot
            .poll_once()
            .await
            .expect("second runtime poll should succeed");

        let diagnostics = second_boot
            .diagnostics()
            .expect("diagnostics should be readable");
        assert_eq!(diagnostics.last_local_block, Some(1));
        assert_eq!(diagnostics.last_external_replayed_block, Some(7_242_624));
        assert_eq!(diagnostics.replay_lag_blocks, Some(0));
    }

    #[tokio::test]
    async fn real_runtime_backpressure_lag_decreases_as_polls_drain_backlog() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );
        let fetch_625 = fetch_from_rpc_fixtures(
            "mainnet_block_7242625_get_block_with_txs.json",
            "mainnet_block_7242625_get_state_update.json",
        );

        let runtime = RealRuntime::new_with_rpc_source(
            Arc::new(MockRpcSource::new_with_delays(
                "mock://rpc",
                vec![
                    (Duration::from_millis(4), Ok(7_242_625)),
                    (Duration::from_millis(1), Ok(7_242_625)),
                    (Duration::from_millis(3), Ok(7_242_625)),
                ],
                vec![
                    (7_242_623, Duration::from_millis(8), Ok(fetch_623.clone())),
                    (7_242_624, Duration::from_millis(2), Ok(fetch_624.clone())),
                    (7_242_625, Duration::from_millis(6), Ok(fetch_625.clone())),
                ],
            )),
            None,
            3,
            1,
            None,
            RpcRetryConfig {
                max_retries: 0,
                base_backoff: Duration::ZERO,
            },
        )
        .expect("runtime should initialize");

        runtime.poll_once().await.expect("poll 1 should succeed");
        let lag_after_first = runtime
            .diagnostics()
            .expect("diagnostics should be readable")
            .replay_lag_blocks;
        runtime.poll_once().await.expect("poll 2 should succeed");
        let lag_after_second = runtime
            .diagnostics()
            .expect("diagnostics should be readable")
            .replay_lag_blocks;
        runtime.poll_once().await.expect("poll 3 should succeed");
        let diagnostics = runtime
            .diagnostics()
            .expect("diagnostics should be readable");

        assert_eq!(lag_after_first, Some(2));
        assert_eq!(lag_after_second, Some(1));
        assert_eq!(diagnostics.replay_lag_blocks, Some(0));
        assert_eq!(diagnostics.success_count, 3);
        assert_eq!(diagnostics.failure_count, 0);
        assert_eq!(diagnostics.last_local_block, Some(3));
        assert_eq!(diagnostics.last_external_replayed_block, Some(7_242_625));
    }

    #[tokio::test]
    async fn real_runtime_backpressure_persists_when_head_growth_matches_capacity() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );

        let runtime = RealRuntime::new_with_rpc_source(
            Arc::new(MockRpcSource::new_with_delays(
                "mock://rpc",
                vec![
                    (Duration::from_millis(2), Ok(7_242_624)),
                    (Duration::from_millis(2), Ok(7_242_625)),
                ],
                vec![
                    (7_242_623, Duration::from_millis(5), Ok(fetch_623.clone())),
                    (7_242_624, Duration::from_millis(5), Ok(fetch_624.clone())),
                ],
            )),
            None,
            2,
            1,
            None,
            RpcRetryConfig {
                max_retries: 0,
                base_backoff: Duration::ZERO,
            },
        )
        .expect("runtime should initialize");

        runtime.poll_once().await.expect("poll 1 should succeed");
        let lag_after_first = runtime
            .diagnostics()
            .expect("diagnostics should be readable")
            .replay_lag_blocks;
        runtime.poll_once().await.expect("poll 2 should succeed");
        let diagnostics = runtime
            .diagnostics()
            .expect("diagnostics should be readable");

        assert_eq!(lag_after_first, Some(1));
        assert_eq!(diagnostics.replay_lag_blocks, Some(1));
        assert_eq!(diagnostics.success_count, 2);
        assert_eq!(diagnostics.failure_count, 0);
        assert_eq!(diagnostics.last_local_block, Some(2));
        assert_eq!(diagnostics.last_external_replayed_block, Some(7_242_624));
    }

    #[test]
    fn replay_pipeline_bootstraps_window_and_tracks_lag() {
        let mut replay = new_replay_pipeline(4, 3, None).expect("pipeline");
        assert_eq!(replay.plan(10), vec![7, 8, 9]);
        assert_eq!(
            replay
                .evaluate_block(7, "0x0")
                .expect("replay step should be continue"),
            ReplayPipelineStep::Continue {
                local_block_number: 1
            }
        );
        replay
            .mark_committed(7, "0xa")
            .expect("commit should update replay state");
        assert_eq!(replay.replay_lag_blocks(10), 3);
        assert_eq!(replay.expected_parent_hash(), Some("0xa"));
    }

    #[test]
    fn state_diff_summary_fingerprint_is_order_stable() {
        let mut diff_a = StarknetStateDiff::default();
        diff_a.storage_diffs.insert(
            ContractAddress::parse("0x2").expect("valid contract address"),
            BTreeMap::from([
                ("0x2".to_string(), StarknetFelt::from(2_u64)),
                ("0x1".to_string(), StarknetFelt::from(1_u64)),
            ]),
        );
        diff_a.storage_diffs.insert(
            ContractAddress::parse("0x1").expect("valid contract address"),
            BTreeMap::from([("0x3".to_string(), StarknetFelt::from(3_u64))]),
        );
        diff_a
            .nonces
            .insert(ContractAddress::parse("0x1").expect("valid contract address"), StarknetFelt::from(9_u64));
        diff_a
            .declared_classes
            .push(ClassHash::parse("0xa").expect("valid class hash"));

        let mut diff_b = StarknetStateDiff::default();
        diff_b.storage_diffs.insert(
            ContractAddress::parse("0x1").expect("valid contract address"),
            BTreeMap::from([("0x3".to_string(), StarknetFelt::from(3_u64))]),
        );
        diff_b.storage_diffs.insert(
            ContractAddress::parse("0x2").expect("valid contract address"),
            BTreeMap::from([
                ("0x1".to_string(), StarknetFelt::from(1_u64)),
                ("0x2".to_string(), StarknetFelt::from(2_u64)),
            ]),
        );
        diff_b
            .nonces
            .insert(ContractAddress::parse("0x1").expect("valid contract address"), StarknetFelt::from(9_u64));
        diff_b
            .declared_classes
            .push(ClassHash::parse("0xa").expect("valid class hash"));

        assert_eq!(summarize_state_diff(&diff_a), summarize_state_diff(&diff_b));
    }

    #[test]
    fn replay_pipeline_reset_for_reorg_rewinds_window() {
        let mut replay = new_replay_pipeline(5, 2, None).expect("pipeline");
        let _ = replay.plan(20);
        assert_eq!(
            replay
                .evaluate_block(16, "0x0")
                .expect("step should continue"),
            ReplayPipelineStep::Continue {
                local_block_number: 1
            }
        );
        replay.mark_committed(16, "0x16").expect("commit 16");
        assert_eq!(
            replay
                .evaluate_block(17, "0xdead")
                .expect_err("unknown ancestor must fail-closed"),
            ReplayPipelineError::DeepReorgBeyondWindow {
                conflicting_external_block: 17,
                observed_parent_hash: "0xdead".to_string(),
                expected_parent_hash: "0x16".to_string(),
                replay_window: 5,
            }
        );
        assert_eq!(replay.plan(20), vec![17, 18]);
        assert_eq!(replay.next_local_block(), 2);
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
                .get(&ContractAddress::parse("0x111").expect("valid contract address"))
                .and_then(|writes| writes.get("0x1"))
                .copied(),
            Some(StarknetFelt::from(2_u64))
        );
        assert_eq!(
            diff.nonces.get(&ContractAddress::parse("0x111").expect("valid contract address")).copied(),
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
                .get(&ContractAddress::parse("0x222").expect("valid contract address"))
                .and_then(|writes| writes.get("0x10"))
                .copied(),
            Some(StarknetFelt::from(0x20_u64))
        );
        assert_eq!(
            diff.nonces.get(&ContractAddress::parse("0x222").expect("valid contract address")).copied(),
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

    #[test]
    fn replay_fixture_mainnet_7242623_ingests_deterministically() {
        let block_with_txs = load_rpc_fixture("mainnet_block_7242623_get_block_with_txs.json");
        let state_update = load_rpc_fixture("mainnet_block_7242623_get_state_update.json");

        let mut warnings = Vec::new();
        let replay = parse_block_with_txs(&block_with_txs, &mut warnings)
            .expect("block fixture must parse successfully");
        assert!(
            warnings.is_empty(),
            "unexpected parse warnings: {warnings:?}"
        );
        assert_eq!(replay.external_block_number, 7_242_623);
        assert_eq!(
            replay.block_hash,
            "0x3f2981fad7283a0af9212c1cc4a6c78ec7040dce2abf82a5b389e7aa18a4bb"
        );
        assert_eq!(replay.transaction_hashes.len(), 3);
        assert_eq!(
            replay.transaction_hashes[0],
            "0x16208227ebe3964d9a055c2995986eb77a53d6a3cc492ca5a5c3a249cd7ec76"
        );

        let (state_diff, state_warnings) =
            state_update_to_diff(&state_update).expect("state update fixture must parse");
        assert!(
            state_warnings.is_empty(),
            "unexpected state diff warnings: {state_warnings:?}"
        );
        assert_eq!(state_diff.storage_diffs.len(), 7);
        assert_eq!(state_diff.nonces.len(), 3);
        assert!(state_diff.declared_classes.is_empty());

        let expected_contract = ContractAddress::parse(
            "0x2dd3209b948554421cdb9bb8791c69d92154aa279e7eb52e9647335072a132d",
        )
        .expect("valid contract address");
        let expected_key = "0x6dfe00e4de355a406222fd8f14f4fa4a266ad915291694ae2827ed3b10b8d1";
        let expected_value = StarknetFelt::from_str("0x69a2dee7").expect("valid felt");
        assert_eq!(
            state_diff
                .storage_diffs
                .get(&expected_contract)
                .and_then(|writes| writes.get(expected_key))
                .copied(),
            Some(expected_value)
        );

        let (state_diff_repeat, repeat_warnings) =
            state_update_to_diff(&state_update).expect("repeat parse must succeed");
        assert_eq!(state_diff_repeat, state_diff);
        assert_eq!(repeat_warnings, state_warnings);

        let fetch = RealFetch {
            snapshot: RealSnapshot {
                chain_id: "SN_MAIN".to_string(),
                latest_block: replay.external_block_number,
                state_root: state_update
                    .get("new_root")
                    .and_then(Value::as_str)
                    .expect("fixture must expose new_root")
                    .to_string(),
                tx_count: replay.transaction_hashes.len() as u64,
                rpc_latency_ms: 0,
                captured_unix_seconds: 0,
            },
            replay,
            state_diff,
            parse_warnings: Vec::new(),
        };
        let ingested = ingest_block_from_fetch(1, &fetch).expect("ingestion must succeed");
        ingested.validate().expect("ingested block must be valid");
        assert_eq!(ingested.transactions.len(), 3);
        assert_eq!(
            ingested.parent_hash,
            "0x8c503be721489518c3dca57399a23581c1ca6ce6e06bb6d9e05896feec30d2"
        );
    }

    #[test]
    fn replay_fixture_mainnet_7242624_ingests_deterministically() {
        let previous_block = load_rpc_fixture("mainnet_block_7242623_get_block_with_txs.json");
        let block_with_txs = load_rpc_fixture("mainnet_block_7242624_get_block_with_txs.json");
        let state_update = load_rpc_fixture("mainnet_block_7242624_get_state_update.json");

        let mut previous_warnings = Vec::new();
        let previous_replay = parse_block_with_txs(&previous_block, &mut previous_warnings)
            .expect("previous fixture must parse");
        assert!(previous_warnings.is_empty());

        let mut warnings = Vec::new();
        let replay = parse_block_with_txs(&block_with_txs, &mut warnings)
            .expect("block fixture must parse successfully");
        assert!(
            warnings.is_empty(),
            "unexpected parse warnings: {warnings:?}"
        );
        assert_eq!(replay.external_block_number, 7_242_624);
        assert_eq!(
            replay.parent_hash, previous_replay.block_hash,
            "captured fixture chain continuity must hold"
        );
        assert_eq!(replay.transaction_hashes.len(), 6);
        assert_eq!(
            replay.transaction_hashes[0],
            "0x183aa10ea20213b3426375d223d667a129e1b1cc03b09e6332c11d3858fda22"
        );

        let (state_diff, state_warnings) =
            state_update_to_diff(&state_update).expect("state update fixture must parse");
        assert!(
            state_warnings.is_empty(),
            "unexpected state diff warnings: {state_warnings:?}"
        );
        assert_eq!(state_diff.storage_diffs.len(), 16);
        assert_eq!(state_diff.nonces.len(), 6);
        assert!(state_diff.declared_classes.is_empty());

        let expected_contract = ContractAddress::parse(
            "0x7229d1454093674673a530cd0d37beef3fc0f1b3116d95c62c5c032f1827d87",
        )
        .expect("valid contract address");
        let expected_key = "0x333aaabd0e6d8fa946806a7af3989c1138cc653163215838c0ef3a9a7f60018";
        let expected_value = StarknetFelt::from_str("0x802ddac7").expect("valid felt");
        assert_eq!(
            state_diff
                .storage_diffs
                .get(&expected_contract)
                .and_then(|writes| writes.get(expected_key))
                .copied(),
            Some(expected_value)
        );

        let fetch = RealFetch {
            snapshot: RealSnapshot {
                chain_id: "SN_MAIN".to_string(),
                latest_block: replay.external_block_number,
                state_root: state_update
                    .get("new_root")
                    .and_then(Value::as_str)
                    .expect("fixture must expose new_root")
                    .to_string(),
                tx_count: replay.transaction_hashes.len() as u64,
                rpc_latency_ms: 0,
                captured_unix_seconds: 0,
            },
            replay,
            state_diff,
            parse_warnings: Vec::new(),
        };
        let ingested = ingest_block_from_fetch(2, &fetch).expect("ingestion must succeed");
        ingested.validate().expect("ingested block must be valid");
        assert_eq!(ingested.transactions.len(), 6);
        assert_eq!(
            ingested.state_root,
            "0x25a23d7704a148a505fcd1d25c58c5a2f4847fbd3a739d809a1d0cfb25f7494"
        );
    }

    #[test]
    fn replay_state_diff_fingerprint_snapshots_for_rpc_fixtures() {
        let fixtures = [
            (
                "mainnet_block_7242623_get_block_with_txs.json",
                "mainnet_block_7242623_get_state_update.json",
                StateDiffSummary {
                    fingerprint: "fnv64:1a0c346877abcc58".to_string(),
                    touched_contracts: 7,
                    storage_writes: 26,
                    nonce_updates: 3,
                    declared_classes: 0,
                },
            ),
            (
                "mainnet_block_7242624_get_block_with_txs.json",
                "mainnet_block_7242624_get_state_update.json",
                StateDiffSummary {
                    fingerprint: "fnv64:75568546f8968975".to_string(),
                    touched_contracts: 16,
                    storage_writes: 59,
                    nonce_updates: 6,
                    declared_classes: 0,
                },
            ),
            (
                "mainnet_block_7242625_get_block_with_txs.json",
                "mainnet_block_7242625_get_state_update.json",
                StateDiffSummary {
                    fingerprint: "fnv64:53f2210220632692".to_string(),
                    touched_contracts: 8,
                    storage_writes: 27,
                    nonce_updates: 4,
                    declared_classes: 0,
                },
            ),
        ];

        for (block_fixture, state_fixture, expected_summary) in fixtures {
            let fetch = fetch_from_rpc_fixtures(block_fixture, state_fixture);
            let summary = summarize_state_diff(&fetch.state_diff);
            assert_eq!(
                summary, expected_summary,
                "state diff summary drifted for fixture {}",
                state_fixture
            );
        }
    }

    #[tokio::test]
    async fn real_runtime_records_last_state_diff_summary_in_diagnostics() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );

        let runtime = Arc::new(
            RealRuntime::new_with_rpc_source(
                Arc::new(MockRpcSource::new(
                    "mock://rpc",
                    vec![Ok(7_242_623)],
                    vec![(7_242_623, Ok(fetch_623.clone()))],
                )),
                None,
                1,
                1,
                None,
                RpcRetryConfig {
                    max_retries: 0,
                    base_backoff: Duration::ZERO,
                },
            )
            .expect("runtime should initialize"),
        );

        runtime.poll_once().await.expect("poll should succeed");
        let diagnostics = runtime
            .diagnostics()
            .expect("diagnostics should be readable");
        let summary = diagnostics
            .last_state_diff
            .expect("state diff summary should be captured after replay commit");
        assert_eq!(summary.fingerprint, "fnv64:1a0c346877abcc58");
        assert_eq!(summary.touched_contracts, 7);
        assert_eq!(summary.storage_writes, 26);
        assert_eq!(summary.nonce_updates, 3);
        assert_eq!(summary.declared_classes, 0);

        let debug = real_debug(&runtime, 2_000).expect("debug payload should be generated");
        assert_eq!(
            debug.last_state_diff_fingerprint.as_deref(),
            Some("fnv64:1a0c346877abcc58")
        );
        assert_eq!(debug.last_state_diff_touched_contracts, Some(7));
        assert_eq!(debug.last_state_diff_storage_writes, Some(26));
        assert_eq!(debug.last_state_diff_nonce_updates, Some(3));
        assert_eq!(debug.last_state_diff_declared_classes, Some(0));
    }

    #[test]
    fn replay_pipeline_applies_real_mainnet_fixture_sequence_deterministically() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );
        let fetch_625 = fetch_from_rpc_fixtures(
            "mainnet_block_7242625_get_block_with_txs.json",
            "mainnet_block_7242625_get_state_update.json",
        );

        let mut node = build_dashboard_node();
        let mut execution_state = InMemoryState::default();
        apply_fetch_to_local_chain(&mut node, &mut execution_state, 1, &fetch_623);
        apply_fetch_to_local_chain(&mut node, &mut execution_state, 2, &fetch_624);
        apply_fetch_to_local_chain(&mut node, &mut execution_state, 3, &fetch_625);

        let latest = node
            .storage
            .latest_block_number()
            .expect("latest block should be readable");
        assert_eq!(latest, 3);

        let block_2 = node
            .storage
            .get_block(BlockId::Number(2))
            .expect("block 2 query should succeed")
            .expect("block 2 should exist");
        assert_eq!(block_2.parent_hash, fetch_624.replay.parent_hash);

        let block_3 = node
            .storage
            .get_block(BlockId::Number(3))
            .expect("block 3 query should succeed")
            .expect("block 3 should exist");
        assert_eq!(block_3.parent_hash, fetch_625.replay.parent_hash);
        assert_eq!(
            block_3.state_root,
            "0x7c1c12b436136bf413dd5e32ce9a4cdf06cdae9d67fe0babdde6a42f1be4484"
        );
        assert_eq!(block_3.transactions.len(), 4);

        let reader = node
            .storage
            .get_state_reader(3)
            .expect("state reader at block 3 should exist");
        let contract = ContractAddress::parse(
            "0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        )
        .expect("valid contract address");
        let key = "0xa93b2f657f711d43133241be6c27adda9539a298e34d644a2c37106a92d49d";
        let expected = StarknetFelt::from_str("0x24188c710b6bd481c0").expect("valid fixture felt");
        assert_eq!(
            reader
                .get_storage(&contract, key)
                .expect("state read should succeed"),
            Some(expected)
        );
    }

    #[test]
    fn replay_pipeline_detects_reorg_and_resets_local_chain_state() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );

        let mut replay = new_replay_pipeline(8, 8, None).expect("pipeline");
        let mut node = build_dashboard_node();
        let mut execution_state = InMemoryState::default();

        for fetch in [&fetch_623, &fetch_624] {
            let local_block_number = match replay
                .evaluate_block(
                    fetch.replay.external_block_number,
                    &fetch.replay.parent_hash,
                )
                .expect("canonical fixture path must continue")
            {
                ReplayPipelineStep::Continue { local_block_number } => local_block_number,
                ReplayPipelineStep::ReorgRecoverable { .. } => {
                    panic!("unexpected reorg in canonical fixture path")
                }
            };
            apply_fetch_to_local_chain(&mut node, &mut execution_state, local_block_number, fetch);
            replay
                .mark_committed(fetch.replay.external_block_number, &fetch.replay.block_hash)
                .expect("mark committed");
        }

        assert_eq!(
            node.storage
                .latest_block_number()
                .expect("latest should be readable before reorg"),
            2
        );

        let mut reorg_block_with_txs =
            load_rpc_fixture("mainnet_block_7242625_get_block_with_txs.json");
        reorg_block_with_txs["parent_hash"] =
            serde_json::Value::String(fetch_623.replay.block_hash.clone());
        let mut parse_warnings = Vec::new();
        let reorg_replay = parse_block_with_txs(&reorg_block_with_txs, &mut parse_warnings)
            .expect("reorg block fixture should parse");
        assert!(parse_warnings.is_empty());

        let reorg_step = replay
            .evaluate_block(
                reorg_replay.external_block_number,
                &reorg_replay.parent_hash,
            )
            .expect("reorg with in-window ancestor should be recoverable");
        match reorg_step {
            ReplayPipelineStep::ReorgRecoverable {
                conflicting_external_block,
                restart_external_block,
                depth,
            } => {
                assert_eq!(conflicting_external_block, 7_242_625);
                assert_eq!(restart_external_block, 7_242_618);
                assert_eq!(depth, 1);
                node = build_dashboard_node();
                execution_state = InMemoryState::default();
            }
            ReplayPipelineStep::Continue { .. } => {
                panic!("reorg branch must trigger replay reset")
            }
        }

        assert_eq!(replay.reorg_events(), 1);
        assert_eq!(replay.next_local_block(), 1);
        assert_eq!(
            node.storage
                .latest_block_number()
                .expect("latest should be readable after reset"),
            0
        );
        assert_eq!(
            replay.plan(7_242_625),
            (7_242_618..=7_242_625).collect::<Vec<_>>()
        );
        assert!(execution_state.storage.is_empty());
    }

    #[test]
    fn replay_pipeline_checkpoint_restores_real_fixture_progress() {
        let dir = tempfile::tempdir().expect("tempdir");
        let checkpoint_path = dir.path().join("replay-checkpoint.json");
        let checkpoint_path_str = checkpoint_path
            .to_str()
            .expect("checkpoint path should be valid utf8");

        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );

        let mut first_boot =
            new_replay_pipeline(8, 8, Some(checkpoint_path_str)).expect("first boot pipeline");
        assert_eq!(
            first_boot
                .evaluate_block(
                    fetch_623.replay.external_block_number,
                    &fetch_623.replay.parent_hash
                )
                .expect("block 7242623 should continue"),
            ReplayPipelineStep::Continue {
                local_block_number: 1
            }
        );
        first_boot
            .mark_committed(
                fetch_623.replay.external_block_number,
                &fetch_623.replay.block_hash,
            )
            .expect("checkpoint should persist committed progress");

        let mut second_boot =
            new_replay_pipeline(8, 8, Some(checkpoint_path_str)).expect("second boot pipeline");
        assert_eq!(second_boot.next_external_block(), Some(7_242_624));
        assert_eq!(
            second_boot.expected_parent_hash(),
            Some(fetch_623.replay.block_hash.as_str())
        );
        assert_eq!(
            second_boot
                .evaluate_block(
                    fetch_624.replay.external_block_number,
                    &fetch_624.replay.parent_hash
                )
                .expect("next block should continue after restore"),
            ReplayPipelineStep::Continue {
                local_block_number: 2
            }
        );
    }

    #[test]
    fn replay_pipeline_fail_closed_on_deep_reorg_from_real_fixture_parent() {
        let fetch_623 = fetch_from_rpc_fixtures(
            "mainnet_block_7242623_get_block_with_txs.json",
            "mainnet_block_7242623_get_state_update.json",
        );
        let fetch_624 = fetch_from_rpc_fixtures(
            "mainnet_block_7242624_get_block_with_txs.json",
            "mainnet_block_7242624_get_state_update.json",
        );

        let mut pipeline = new_replay_pipeline(4, 8, None).expect("pipeline");
        for fetch in [&fetch_623, &fetch_624] {
            assert!(matches!(
                pipeline
                    .evaluate_block(
                        fetch.replay.external_block_number,
                        &fetch.replay.parent_hash
                    )
                    .expect("canonical path should continue"),
                ReplayPipelineStep::Continue { .. }
            ));
            pipeline
                .mark_committed(fetch.replay.external_block_number, &fetch.replay.block_hash)
                .expect("commit should update cursor");
        }

        let err = pipeline
            .evaluate_block(7_242_625, "0xdeadbeef")
            .expect_err("deep reorg ancestor outside hash window must fail-closed");
        assert!(matches!(
            err,
            ReplayPipelineError::DeepReorgBeyondWindow {
                conflicting_external_block: 7_242_625,
                ..
            }
        ));
        assert_eq!(pipeline.reorg_events(), 0);
        assert_eq!(pipeline.next_local_block(), 3);
    }

    #[tokio::test]
    async fn real_rpc_client_call_surfaces_http_failure_payload() {
        async fn failing_handler() -> (StatusCode, Json<Value>) {
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32000, "message": "upstream offline"}
                })),
            )
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind rpc test listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let app = Router::new().route("/", axum::routing::post(failing_handler));
            axum::serve(listener, app)
                .await
                .expect("rpc test server should run");
        });

        let client = RealRpcClient::new(format!("http://{addr}")).expect("client");
        let err = client
            .call("starknet_chainId", json!([]))
            .await
            .expect_err("http failure must be surfaced");
        assert!(err.contains("HTTP 502"), "unexpected error: {err}");
        assert!(err.contains("upstream offline"), "unexpected error: {err}");

        server.abort();
    }

    #[tokio::test]
    async fn real_rpc_client_call_surfaces_jsonrpc_error_payload() {
        async fn error_payload_handler() -> Json<Value> {
            Json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": 24, "message": "rate limited"}
            }))
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind rpc test listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let app = Router::new().route("/", axum::routing::post(error_payload_handler));
            axum::serve(listener, app)
                .await
                .expect("rpc test server should run");
        });

        let client = RealRpcClient::new(format!("http://{addr}")).expect("client");
        let err = client
            .call("starknet_getBlockWithTxs", json!([{"block_number": 1}]))
            .await
            .expect_err("jsonrpc error payload must be surfaced");
        assert!(err.contains("error payload"), "unexpected error: {err}");
        assert!(err.contains("rate limited"), "unexpected error: {err}");

        server.abort();
    }
}
