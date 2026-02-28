use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use semver::Version;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::replay::{
    FileReplayCheckpointStore, ReplayPipeline, ReplayPipelineError, ReplayPipelineStep,
};
use crate::{ChainId, NodeConfig, StarknetNode, StarknetNodeBuilder};
use starknet_node_execution::{
    DualExecutionBackend, ExecutionBackend, ExecutionError, ExecutionMode, MismatchPolicy,
};
use starknet_node_storage::{InMemoryStorage, StorageBackend, ThreadSafeStorage};
use starknet_node_types::{
    BlockContext, BlockGasPrices, BuiltinStats, ClassHash, ContractAddress, ExecutionOutput,
    GasPricePerToken, InMemoryState, MutableState, SimulationResult, StarknetBlock, StarknetFelt,
    StarknetReceipt, StarknetStateDiff, StarknetTransaction, StateReader, TxHash,
};

pub const DEFAULT_SYNC_POLL_MS: u64 = 1_500;
pub const DEFAULT_REPLAY_WINDOW: u64 = 64;
pub const DEFAULT_MAX_REPLAY_PER_POLL: u64 = 16;
pub const DEFAULT_RPC_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_RPC_MAX_RETRIES: u32 = 3;
pub const DEFAULT_RPC_RETRY_BACKOFF_MS: u64 = 250;
const MAX_RECENT_ERRORS: usize = 128;

#[derive(Debug, Clone)]
pub struct RpcRetryConfig {
    pub max_retries: u32,
    pub base_backoff: Duration,
}

impl Default for RpcRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_RPC_MAX_RETRIES,
            base_backoff: Duration::from_millis(DEFAULT_RPC_RETRY_BACKOFF_MS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub chain_id: ChainId,
    pub upstream_rpc_url: String,
    pub replay_window: u64,
    pub max_replay_per_poll: u64,
    pub replay_checkpoint_path: Option<String>,
    pub poll_interval: Duration,
    pub rpc_timeout: Duration,
    pub retry: RpcRetryConfig,
}

impl RuntimeConfig {
    fn validate(&self) -> Result<(), String> {
        if self.upstream_rpc_url.trim().is_empty() {
            return Err("upstream_rpc_url cannot be empty".to_string());
        }
        if self.replay_window == 0 {
            return Err("replay_window must be > 0".to_string());
        }
        if self.max_replay_per_poll == 0 {
            return Err("max_replay_per_poll must be > 0".to_string());
        }
        if self.poll_interval.is_zero() {
            return Err("poll_interval must be > 0".to_string());
        }
        if self.rpc_timeout.is_zero() {
            return Err("rpc_timeout must be > 0".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SyncProgress {
    pub starting_block: u64,
    pub current_block: u64,
    pub highest_block: u64,
    pub reorg_events: u64,
    pub consecutive_failures: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RuntimeDiagnostics {
    pub success_count: u64,
    pub failure_count: u64,
    pub consecutive_failures: u64,
    pub replayed_tx_count: u64,
    pub replay_failures: u64,
    pub execution_failures: u64,
    pub commit_failures: u64,
    pub reorg_events: u64,
    pub last_error: Option<String>,
    pub recent_errors: Vec<String>,
}

type RuntimeNode = StarknetNode<ThreadSafeStorage<InMemoryStorage>, DualExecutionBackend>;

pub struct NodeRuntime {
    client: UpstreamRpcClient,
    node: RuntimeNode,
    execution_state: InMemoryState,
    replay: ReplayPipeline,
    poll_interval: Duration,
    retry: RpcRetryConfig,
    diagnostics: RuntimeDiagnostics,
    recent_errors: VecDeque<String>,
    sync_progress: Arc<Mutex<SyncProgress>>,
    chain_id_validated: bool,
}

impl NodeRuntime {
    pub fn new(config: RuntimeConfig) -> Result<Self, String> {
        config.validate()?;

        let client = UpstreamRpcClient::new(config.upstream_rpc_url.clone(), config.rpc_timeout)?;
        let node = build_runtime_node(config.chain_id.clone());

        let mut replay = new_replay_pipeline(
            config.replay_window,
            config.max_replay_per_poll,
            config.replay_checkpoint_path.as_deref(),
        )
        .map_err(|error| format!("failed to initialize replay pipeline: {error}"))?;

        let storage_tip = node
            .storage
            .latest_block_number()
            .map_err(|error| format!("failed to read storage tip: {error}"))?;
        let expected_next_local = storage_tip.saturating_add(1);

        if replay.next_local_block() != expected_next_local {
            if storage_tip == 0 {
                if let Some(path) = config.replay_checkpoint_path.as_deref() {
                    match std::fs::remove_file(path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            return Err(format!(
                                "failed to remove stale replay checkpoint at {path}: {error}"
                            ));
                        }
                    }
                    replay = new_replay_pipeline(
                        config.replay_window,
                        config.max_replay_per_poll,
                        Some(path),
                    )
                    .map_err(|error| {
                        format!(
                            "failed to reinitialize replay pipeline after stale checkpoint reset: {error}"
                        )
                    })?;
                }
            } else {
                return Err(format!(
                    "replay checkpoint local cursor {} mismatches local storage tip {}; refusing to start",
                    replay.next_local_block(),
                    storage_tip
                ));
            }
        }

        let mut progress = SyncProgress {
            starting_block: storage_tip,
            current_block: storage_tip,
            highest_block: storage_tip,
            ..SyncProgress::default()
        };
        progress.reorg_events = replay.reorg_events();

        Ok(Self {
            client,
            node,
            execution_state: InMemoryState::default(),
            replay,
            poll_interval: config.poll_interval,
            retry: config.retry,
            diagnostics: RuntimeDiagnostics::default(),
            recent_errors: VecDeque::new(),
            sync_progress: Arc::new(Mutex::new(progress)),
            chain_id_validated: false,
        })
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub fn chain_id(&self) -> &str {
        self.node.config.chain_id.as_str()
    }

    pub fn storage(&self) -> ThreadSafeStorage<InMemoryStorage> {
        self.node.storage.clone()
    }

    pub fn sync_progress_handle(&self) -> Arc<Mutex<SyncProgress>> {
        Arc::clone(&self.sync_progress)
    }

    pub fn diagnostics(&self) -> RuntimeDiagnostics {
        self.diagnostics.clone()
    }

    pub async fn poll_once(&mut self) -> Result<(), String> {
        self.ensure_chain_id_validated().await?;

        let external_head_block = self.fetch_latest_block_number_with_retry().await?;
        self.update_sync_progress(|progress| {
            progress.highest_block = external_head_block;
        })?;

        let replay_plan = self.replay.plan(external_head_block);
        if replay_plan.is_empty() {
            let latest_local = self
                .node
                .storage
                .latest_block_number()
                .map_err(|error| format!("failed to read local block number: {error}"))?;
            self.update_sync_progress(|progress| {
                progress.current_block = latest_local;
            })?;
            self.record_success();
            return Ok(());
        }

        for external_block in replay_plan {
            let fetch = self.fetch_block_with_retry(external_block).await?;
            let local_block_number = match self
                .replay
                .evaluate_block(external_block, &fetch.replay.parent_hash)
            {
                Ok(ReplayPipelineStep::Continue { local_block_number }) => local_block_number,
                Ok(ReplayPipelineStep::ReorgRecoverable {
                    conflicting_external_block,
                    restart_external_block,
                    depth,
                }) => {
                    self.diagnostics.replay_failures =
                        self.diagnostics.replay_failures.saturating_add(1);
                    let message = format!(
                        "reorg detected at external block {conflicting_external_block} (depth={depth}, restart={restart_external_block}); append-only local storage requires operator intervention"
                    );
                    self.record_failure(message.clone());
                    return Err(message);
                }
                Err(error) => {
                    self.diagnostics.replay_failures =
                        self.diagnostics.replay_failures.saturating_add(1);
                    let message = format!(
                        "replay guardrail failure at external block {external_block}: {error}"
                    );
                    self.record_failure(message.clone());
                    return Err(message);
                }
            };

            let local_block = ingest_block_from_fetch(local_block_number, &fetch).map_err(|error| {
                let message = format!(
                    "failed to ingest external block {external_block} as local block {local_block_number}: {error}"
                );
                self.diagnostics.replay_failures =
                    self.diagnostics.replay_failures.saturating_add(1);
                self.record_failure(message.clone());
                message
            })?;

            if let Err(error) = self
                .node
                .execution
                .execute_block(&local_block, &mut self.execution_state)
            {
                self.diagnostics.execution_failures =
                    self.diagnostics.execution_failures.saturating_add(1);
                let message = format!(
                    "execution failed for local block {local_block_number} (external {external_block}): {error}"
                );
                self.record_failure(message.clone());
                return Err(message);
            }

            if let Err(error) = self
                .node
                .storage
                .insert_block(local_block, fetch.state_diff.clone())
            {
                self.diagnostics.commit_failures =
                    self.diagnostics.commit_failures.saturating_add(1);
                let message = format!(
                    "storage commit failed for local block {local_block_number} (external {external_block}): {error}"
                );
                self.record_failure(message.clone());
                return Err(message);
            }

            if let Err(error) = self
                .replay
                .mark_committed(external_block, &fetch.replay.block_hash)
            {
                self.diagnostics.replay_failures =
                    self.diagnostics.replay_failures.saturating_add(1);
                let message = format!(
                    "failed to persist replay checkpoint after external block {external_block}: {error}"
                );
                self.record_failure(message.clone());
                return Err(message);
            }

            self.diagnostics.replayed_tx_count = self
                .diagnostics
                .replayed_tx_count
                .saturating_add(fetch.replay.transaction_hashes.len() as u64);

            self.update_sync_progress(|progress| {
                progress.current_block = external_block;
                progress.reorg_events = self.replay.reorg_events();
                progress.last_error = None;
            })?;
        }

        self.record_success();
        Ok(())
    }

    async fn fetch_latest_block_number_with_retry(&self) -> Result<u64, String> {
        let mut attempt = 0_u32;
        loop {
            match self.client.fetch_latest_block_number().await {
                Ok(head) => return Ok(head),
                Err(error) => {
                    if attempt >= self.retry.max_retries {
                        return Err(format!(
                            "latest block RPC failed after {} attempts: {error}",
                            self.retry.max_retries.saturating_add(1)
                        ));
                    }
                    let backoff = self.retry_backoff(attempt);
                    if !backoff.is_zero() {
                        sleep(backoff).await;
                    }
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }

    async fn fetch_block_with_retry(&self, block_number: u64) -> Result<RuntimeFetch, String> {
        let mut attempt = 0_u32;
        loop {
            match self.client.fetch_block(block_number).await {
                Ok(fetch) => return Ok(fetch),
                Err(error) => {
                    if attempt >= self.retry.max_retries {
                        return Err(format!(
                            "block {block_number} RPC failed after {} attempts: {error}",
                            self.retry.max_retries.saturating_add(1)
                        ));
                    }
                    let backoff = self.retry_backoff(attempt);
                    if !backoff.is_zero() {
                        sleep(backoff).await;
                    }
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }

    async fn ensure_chain_id_validated(&mut self) -> Result<(), String> {
        if self.chain_id_validated {
            return Ok(());
        }
        let upstream_chain_id = self.client.fetch_chain_id().await?;
        let local_chain_id = self.chain_id().to_string();
        if normalize_chain_id(&upstream_chain_id) != normalize_chain_id(&local_chain_id) {
            let message = format!(
                "chain id mismatch: local={} upstream={}",
                local_chain_id, upstream_chain_id
            );
            self.record_failure(message.clone());
            return Err(message);
        }
        self.chain_id_validated = true;
        Ok(())
    }

    fn retry_backoff(&self, attempt: u32) -> Duration {
        if self.retry.base_backoff.is_zero() {
            return Duration::ZERO;
        }
        let factor = 1_u128 << attempt.min(20);
        let base_ms = self.retry.base_backoff.as_millis();
        let backoff_ms = base_ms.saturating_mul(factor).min(5_000);
        Duration::from_millis(backoff_ms as u64)
    }

    fn record_failure(&mut self, message: String) {
        self.diagnostics.failure_count = self.diagnostics.failure_count.saturating_add(1);
        self.diagnostics.consecutive_failures =
            self.diagnostics.consecutive_failures.saturating_add(1);
        self.diagnostics.last_error = Some(message.clone());
        push_recent_error(&mut self.recent_errors, message.clone());
        self.diagnostics.recent_errors = self.recent_errors.iter().cloned().collect();

        let _ = self.update_sync_progress(|progress| {
            progress.consecutive_failures = progress.consecutive_failures.saturating_add(1);
            progress.last_error = Some(message);
            progress.reorg_events = self.replay.reorg_events();
        });
    }

    fn record_success(&mut self) {
        self.diagnostics.success_count = self.diagnostics.success_count.saturating_add(1);
        self.diagnostics.consecutive_failures = 0;
        self.diagnostics.last_error = None;
        self.diagnostics.recent_errors = self.recent_errors.iter().cloned().collect();
        let _ = self.update_sync_progress(|progress| {
            progress.consecutive_failures = 0;
            progress.last_error = None;
            progress.reorg_events = self.replay.reorg_events();
        });
    }

    fn update_sync_progress(&self, update: impl FnOnce(&mut SyncProgress)) -> Result<(), String> {
        let mut guard = self
            .sync_progress
            .lock()
            .map_err(|_| "sync progress lock poisoned".to_string())?;
        update(&mut guard);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct RuntimeBlockReplay {
    external_block_number: u64,
    block_hash: String,
    parent_hash: String,
    sequencer_address: String,
    timestamp: u64,
    transaction_hashes: Vec<String>,
}

#[derive(Debug, Clone)]
struct RuntimeFetch {
    replay: RuntimeBlockReplay,
    state_diff: StarknetStateDiff,
    state_root: String,
}

#[derive(Clone)]
struct UpstreamRpcClient {
    http: reqwest::Client,
    rpc_url: String,
}

impl UpstreamRpcClient {
    fn new(rpc_url: String, timeout: Duration) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
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

    async fn fetch_block(&self, block_number: u64) -> Result<RuntimeFetch, String> {
        let block_selector = json!([{ "block_number": block_number }]);

        let block_with_txs = self
            .call("starknet_getBlockWithTxs", block_selector.clone())
            .await?;

        let mut warnings = Vec::new();
        let replay = parse_block_with_txs(&block_with_txs, &mut warnings)?;
        if replay.external_block_number != block_number {
            warnings.push(format!(
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
            warnings.push(format!(
                "tx count mismatch between getBlockTransactionCount ({tx_count_from_rpc}) and getBlockWithTxs ({tx_count_from_replay})"
            ));
        }

        let state_update = self
            .call("starknet_getStateUpdate", block_selector.clone())
            .await?;
        let state_root = state_update
            .get("new_root")
            .and_then(Value::as_str)
            .unwrap_or("0x0")
            .to_string();
        let (state_diff, mut state_diff_warnings) = state_update_to_diff(&state_update)?;
        warnings.append(&mut state_diff_warnings);

        if !warnings.is_empty() {
            let joined = warnings.join(" | ");
            eprintln!("warning: block {block_number} parse issues: {joined}");
        }

        Ok(RuntimeFetch {
            replay,
            state_diff,
            state_root,
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

fn build_runtime_node(chain_id: ChainId) -> RuntimeNode {
    let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
    StarknetNodeBuilder::new(NodeConfig { chain_id })
        .with_storage(storage)
        .with_execution(build_runtime_execution_backend())
        .with_rpc(true)
        .build()
}

fn build_runtime_execution_backend() -> DualExecutionBackend {
    DualExecutionBackend::new(
        Some(Box::new(RuntimeExecutionBackend)),
        Box::new(RuntimeExecutionBackend),
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

struct RuntimeExecutionBackend;

impl ExecutionBackend for RuntimeExecutionBackend {
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

fn ingest_block_from_fetch(
    local_number: u64,
    fetch: &RuntimeFetch,
) -> Result<StarknetBlock, String> {
    let replay = &fetch.replay;
    let parent_hash = StarknetFelt::from_str(&replay.parent_hash)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!(
                "invalid replay parent hash `{}`: {error}",
                replay.parent_hash
            )
        })?;
    let state_root = StarknetFelt::from_str(&fetch.state_root)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| format!("invalid replay state root `{}`: {error}", fetch.state_root))?;
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
        sequencer_address: ContractAddress::parse(sequencer_address)
            .expect("canonicalized sequencer address must be valid"),
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
        protocol_version: Version::parse("0.14.2")
            .expect("runtime protocol version literal must be valid"),
        transactions,
    };
    block
        .validate()
        .map_err(|error| format!("invalid replay block {local_number}: {error}"))?;
    Ok(block)
}

fn parse_block_with_txs(
    block_with_txs: &Value,
    warnings: &mut Vec<String>,
) -> Result<RuntimeBlockReplay, String> {
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

    if let Some(status_raw) = block_with_txs.get("status").and_then(Value::as_str) {
        if status_raw.eq_ignore_ascii_case("PENDING") {
            return Err(
                "getBlockWithTxs returned status=PENDING; daemon only replays finalized blocks"
                    .to_string(),
            );
        }
        if !status_raw.eq_ignore_ascii_case("ACCEPTED_ON_L2")
            && !status_raw.eq_ignore_ascii_case("ACCEPTED_ON_L1")
            && !status_raw.eq_ignore_ascii_case("FINALIZED")
        {
            warnings.push(format!(
                "unexpected block status `{status_raw}`; replay continues but should be verified"
            ));
        }
    }

    Ok(RuntimeBlockReplay {
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
                warnings.push(format!(
                    "invalid canonical contract address `{raw}`: {error}"
                ));
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

fn push_recent_error(errors: &mut VecDeque<String>, message: String) {
    if errors.len() >= MAX_RECENT_ERRORS {
        let _ = errors.pop_front();
    }
    errors.push_back(message);
}

fn normalize_chain_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        let normalized_hex = hex.trim_start_matches('0').to_ascii_lowercase();
        if let Some(decoded) = decode_ascii_hex(hex) {
            return decoded;
        }
        if normalized_hex.is_empty() {
            return "0x0".to_string();
        }
        return format!("0x{normalized_hex}");
    }
    trimmed.to_string()
}

fn decode_ascii_hex(hex: &str) -> Option<String> {
    if hex.is_empty() || hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.chars();
    while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
        let high = hi.to_digit(16)? as u8;
        let low = lo.to_digit(16)? as u8;
        let value = (high << 4) | low;
        if value == 0 || !value.is_ascii() {
            return None;
        }
        bytes.push(value);
    }
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_block_with_txs_hashes_from_objects() {
        let block = json!({
            "block_number": 12,
            "block_hash": "0xabc",
            "parent_hash": "0x123",
            "sequencer_address": "0x1",
            "timestamp": 1_700_000_012_u64,
            "transactions": [
                {"transaction_hash": "0x1"},
                {"transaction_hash": "0x2"}
            ]
        });

        let mut warnings = Vec::new();
        let parsed = parse_block_with_txs(&block, &mut warnings).expect("must parse");
        assert_eq!(parsed.external_block_number, 12);
        assert_eq!(parsed.transaction_hashes.len(), 2);
        assert!(warnings.is_empty());
    }

    #[test]
    fn converts_state_update_to_state_diff() {
        let state_update = json!({
            "state_diff": {
                "storage_diffs": {
                    "0x1": [
                        {"key": "0x10", "value": "0x99"}
                    ]
                },
                "nonces": {
                    "0x1": "0x1"
                },
                "declared_classes": [
                    {"class_hash": "0x100"}
                ]
            }
        });

        let (diff, warnings) = state_update_to_diff(&state_update).expect("must parse");
        assert_eq!(diff.storage_diffs.len(), 1);
        assert_eq!(diff.nonces.len(), 1);
        assert_eq!(diff.declared_classes.len(), 1);
        assert!(warnings.is_empty());
    }

    #[test]
    fn normalize_chain_id_accepts_hex_encoded_ascii() {
        assert_eq!(normalize_chain_id("SN_MAIN"), "SN_MAIN");
        assert_eq!(normalize_chain_id("0x534e5f4d41494e"), "SN_MAIN");
    }
}
