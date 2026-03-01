use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use semver::Version;
use serde::{Deserialize, Serialize};
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
const MAX_UPSTREAM_RPC_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_LOCAL_JOURNAL_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_LOCAL_JOURNAL_LINE_BYTES: usize = 4 * 1024 * 1024;
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
    pub peer_count_hint: usize,
    pub require_peers: bool,
    pub local_journal_path: Option<String>,
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
        if let Some(path) = self.local_journal_path.as_deref()
            && path.trim().is_empty()
        {
            return Err("local_journal_path cannot be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SyncProgress {
    pub starting_block: u64,
    pub current_block: u64,
    pub highest_block: u64,
    pub peer_count: u64,
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
    pub consensus_rejections: u64,
    pub network_failures: u64,
    pub journal_failures: u64,
    pub execution_failures: u64,
    pub commit_failures: u64,
    pub reorg_events: u64,
    pub last_error: Option<String>,
    pub recent_errors: Vec<String>,
}

type RuntimeNode = StarknetNode<ThreadSafeStorage<InMemoryStorage>, DualExecutionBackend>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalJournalEntry {
    block: StarknetBlock,
    state_diff: StarknetStateDiff,
}

#[derive(Debug, Clone)]
struct LocalChainJournal {
    path: PathBuf,
}

impl LocalChainJournal {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn load_entries(&self, limit: Option<u64>) -> Result<Vec<LocalJournalEntry>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let metadata = fs::metadata(&self.path).map_err(|error| {
            format!(
                "failed to inspect local journal {}: {error}",
                self.path.display()
            )
        })?;
        if metadata.len() > MAX_LOCAL_JOURNAL_FILE_BYTES {
            return Err(format!(
                "local journal {} size {} exceeds max allowed {} bytes",
                self.path.display(),
                metadata.len(),
                MAX_LOCAL_JOURNAL_FILE_BYTES
            ));
        }
        let file = File::open(&self.path).map_err(|error| {
            format!(
                "failed to open local journal {}: {error}",
                self.path.display()
            )
        })?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for (idx, line) in reader.lines().enumerate() {
            let line = line.map_err(|error| {
                format!(
                    "failed to read local journal {} line {}: {error}",
                    self.path.display(),
                    idx + 1
                )
            })?;
            if line.len() > MAX_LOCAL_JOURNAL_LINE_BYTES {
                return Err(format!(
                    "local journal {} line {} exceeds max allowed {} bytes",
                    self.path.display(),
                    idx + 1,
                    MAX_LOCAL_JOURNAL_LINE_BYTES
                ));
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let entry = serde_json::from_str::<LocalJournalEntry>(trimmed).map_err(|error| {
                format!(
                    "failed to decode local journal {} line {}: {error}",
                    self.path.display(),
                    idx + 1
                )
            })?;
            entries.push(entry);
            if let Some(limit) = limit
                && entries.len() as u64 >= limit
            {
                break;
            }
        }
        Ok(entries)
    }

    fn append_entry(&self, entry: &LocalJournalEntry) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create local journal directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| {
                format!(
                    "failed to open local journal for append {}: {error}",
                    self.path.display()
                )
            })?;
        let encoded = serde_json::to_string(entry).map_err(|error| {
            format!(
                "failed to encode local journal entry for {}: {error}",
                self.path.display()
            )
        })?;
        file.write_all(encoded.as_bytes()).map_err(|error| {
            format!(
                "failed to write local journal entry to {}: {error}",
                self.path.display()
            )
        })?;
        file.write_all(b"\n").map_err(|error| {
            format!(
                "failed to write local journal newline to {}: {error}",
                self.path.display()
            )
        })?;
        file.sync_data().map_err(|error| {
            format!(
                "failed to fsync local journal {}: {error}",
                self.path.display()
            )
        })?;
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
struct RuntimeBlockHashes {
    external_block_number: u64,
    block_hash: String,
    parent_hash: String,
    transaction_hashes: Vec<String>,
}

#[derive(Debug, Clone)]
struct RuntimeFetch {
    replay: RuntimeBlockReplay,
    state_diff: StarknetStateDiff,
    state_root: String,
}

type SyncSourceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

trait SyncSource: Send + Sync {
    fn fetch_latest_block_number(&self) -> SyncSourceFuture<'_, u64>;
    fn fetch_chain_id(&self) -> SyncSourceFuture<'_, String>;
    fn fetch_block(&self, block_number: u64) -> SyncSourceFuture<'_, RuntimeFetch>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConsensusInput {
    external_block_number: u64,
    block_hash: String,
    parent_hash: String,
    timestamp: u64,
    tx_count: usize,
}

impl From<&RuntimeBlockReplay> for ConsensusInput {
    fn from(value: &RuntimeBlockReplay) -> Self {
        Self {
            external_block_number: value.external_block_number,
            block_hash: value.block_hash.clone(),
            parent_hash: value.parent_hash.clone(),
            timestamp: value.timestamp,
            tx_count: value.transaction_hashes.len(),
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsensusVerdict {
    Accept,
    Reject,
}

trait ConsensusBackend: Send + Sync {
    fn validate_block(&self, input: &ConsensusInput) -> Result<ConsensusVerdict, String>;
}

struct AllowAllConsensusBackend;

impl ConsensusBackend for AllowAllConsensusBackend {
    fn validate_block(&self, _input: &ConsensusInput) -> Result<ConsensusVerdict, String> {
        Ok(ConsensusVerdict::Accept)
    }
}

trait NetworkBackend: Send + Sync {
    fn is_healthy(&self) -> bool;
    fn peer_count(&self) -> usize;
}

struct StaticNetworkBackend {
    peer_count: usize,
    healthy: bool,
}

impl StaticNetworkBackend {
    fn from_config(peer_count: usize, require_peers: bool) -> Self {
        let healthy = !require_peers || peer_count > 0;
        Self {
            peer_count,
            healthy,
        }
    }
}

impl NetworkBackend for StaticNetworkBackend {
    fn is_healthy(&self) -> bool {
        self.healthy
    }

    fn peer_count(&self) -> usize {
        self.peer_count
    }
}

pub struct NodeRuntime {
    sync_source: Arc<dyn SyncSource>,
    consensus: Arc<dyn ConsensusBackend>,
    network: Arc<dyn NetworkBackend>,
    node: RuntimeNode,
    execution_state: InMemoryState,
    journal: Option<LocalChainJournal>,
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
        let sync_source = Arc::new(UpstreamRpcClient::new(
            config.upstream_rpc_url.clone(),
            config.rpc_timeout,
        )?);
        let consensus = Arc::new(AllowAllConsensusBackend);
        let network = Arc::new(StaticNetworkBackend::from_config(
            config.peer_count_hint,
            config.require_peers,
        ));
        Self::new_with_backends(config, sync_source, consensus, network)
    }

    fn new_with_backends(
        config: RuntimeConfig,
        sync_source: Arc<dyn SyncSource>,
        consensus: Arc<dyn ConsensusBackend>,
        network: Arc<dyn NetworkBackend>,
    ) -> Result<Self, String> {
        config.validate()?;
        let mut replay = new_replay_pipeline(
            config.replay_window,
            config.max_replay_per_poll,
            config.replay_checkpoint_path.as_deref(),
        )
        .map_err(|error| format!("failed to initialize replay pipeline: {error}"))?;
        let mut node = build_runtime_node(config.chain_id.clone());
        let mut execution_state = InMemoryState::default();
        let journal = config
            .local_journal_path
            .as_deref()
            .map(LocalChainJournal::new);
        if let Some(journal) = journal.as_ref() {
            let target_local_blocks = replay.next_local_block().saturating_sub(1);
            let restored = restore_storage_from_journal(
                &mut node,
                &mut execution_state,
                journal,
                Some(target_local_blocks),
            )?;
            if restored < target_local_blocks {
                return Err(format!(
                    "local journal {} has {restored} entries but replay checkpoint requires {target_local_blocks}",
                    journal.path().display()
                ));
            }
        }

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
            sync_source,
            consensus,
            network,
            node,
            execution_state,
            journal,
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

        let peer_count = self.network.peer_count() as u64;
        self.update_sync_progress(|progress| {
            progress.peer_count = peer_count;
        })?;
        if !self.network.is_healthy() {
            self.diagnostics.network_failures = self.diagnostics.network_failures.saturating_add(1);
            let message = format!("network backend unhealthy (peer_count={peer_count})");
            self.record_failure(message.clone());
            return Err(message);
        }

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
            let consensus_input = ConsensusInput::from(&fetch.replay);
            let consensus_verdict = self.consensus.validate_block(&consensus_input).map_err(
                |error| {
                    let message = format!(
                        "consensus validation failed at external block {external_block}: {error}"
                    );
                    self.record_failure(message.clone());
                    message
                },
            )?;
            if matches!(consensus_verdict, ConsensusVerdict::Reject) {
                self.diagnostics.consensus_rejections =
                    self.diagnostics.consensus_rejections.saturating_add(1);
                let message = format!(
                    "consensus rejected external block {}",
                    consensus_input.external_block_number
                );
                self.record_failure(message.clone());
                return Err(message);
            }
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
            let local_block_for_journal = local_block.clone();

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

            if let Some(journal) = &self.journal {
                let journal_entry = LocalJournalEntry {
                    block: local_block_for_journal,
                    state_diff: fetch.state_diff.clone(),
                };
                if let Err(error) = journal.append_entry(&journal_entry) {
                    self.diagnostics.journal_failures =
                        self.diagnostics.journal_failures.saturating_add(1);
                    let message = format!(
                        "local journal append failed before committing local block {local_block_number}: {error}"
                    );
                    self.record_failure(message.clone());
                    return Err(message);
                }
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
            match self.sync_source.fetch_latest_block_number().await {
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
            match self.sync_source.fetch_block(block_number).await {
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
        let upstream_chain_id = self.fetch_chain_id_with_retry().await?;
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

    async fn fetch_chain_id_with_retry(&self) -> Result<String, String> {
        let mut attempt = 0_u32;
        loop {
            match self.sync_source.fetch_chain_id().await {
                Ok(chain_id) => return Ok(chain_id),
                Err(error) => {
                    if attempt >= self.retry.max_retries {
                        return Err(format!(
                            "chain id RPC failed after {} attempts: {error}",
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

        let (block_with_txs, tx_count_raw, block_with_hashes, state_update) = tokio::try_join!(
            self.call("starknet_getBlockWithTxs", block_selector.clone()),
            self.call("starknet_getBlockTransactionCount", block_selector.clone()),
            self.call("starknet_getBlockWithTxHashes", block_selector.clone()),
            self.call("starknet_getStateUpdate", block_selector),
        )?;

        let mut warnings = Vec::new();
        let replay = parse_block_with_txs(&block_with_txs, &mut warnings)?;
        if replay.external_block_number != block_number {
            warnings.push(format!(
                "block number mismatch between request ({block_number}) and getBlockWithTxs ({})",
                replay.external_block_number
            ));
        }

        let hashes_view = parse_block_with_tx_hashes(&block_with_hashes, &mut warnings)?;
        compare_block_views(&replay, &hashes_view, &mut warnings);

        let tx_count_from_rpc = value_as_u64(&tx_count_raw)
            .ok_or_else(|| format!("invalid tx count payload: {tx_count_raw}"))?;
        let tx_count_from_replay = replay.transaction_hashes.len() as u64;
        if tx_count_from_rpc != tx_count_from_replay {
            warnings.push(format!(
                "tx count mismatch between getBlockTransactionCount ({tx_count_from_rpc}) and getBlockWithTxs ({tx_count_from_replay})"
            ));
        }
        let state_root = parse_state_root_from_state_update(&state_update)?;
        let (state_diff, mut state_diff_warnings) = state_update_to_diff(&state_update)?;
        warnings.append(&mut state_diff_warnings);

        if !warnings.is_empty() {
            let joined = warnings.join(" | ");
            return Err(format!(
                "block {block_number} failed strict parsing checks: {joined}"
            ));
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

        let mut response = self
            .http
            .post(&self.rpc_url)
            .json(&request)
            .send()
            .await
            .map_err(|error| format!("RPC {method} request failed: {error}"))?;
        let http_status = response.status();
        let content_length = response.content_length();
        let body = read_json_body_with_limit(
            &mut response,
            content_length,
            MAX_UPSTREAM_RPC_RESPONSE_BYTES,
            method,
        )
        .await?;

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

async fn read_json_body_with_limit(
    response: &mut reqwest::Response,
    content_length: Option<u64>,
    max_bytes: usize,
    method: &str,
) -> Result<Value, String> {
    if let Some(length) = content_length
        && length > max_bytes as u64
    {
        return Err(format!(
            "RPC {method} response too large: content-length={length} exceeds {max_bytes} bytes"
        ));
    }
    let mut buffer = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("RPC {method} failed reading response chunk: {error}"))?
    {
        append_limited_chunk(&mut buffer, &chunk, max_bytes, method)?;
    }
    serde_json::from_slice(&buffer)
        .map_err(|error| format!("RPC {method} invalid JSON response: {error}"))
}

fn append_limited_chunk(
    buffer: &mut Vec<u8>,
    chunk: &[u8],
    max_bytes: usize,
    method: &str,
) -> Result<(), String> {
    let new_len = buffer
        .len()
        .checked_add(chunk.len())
        .ok_or_else(|| format!("RPC {method} response size overflow"))?;
    if new_len > max_bytes {
        return Err(format!(
            "RPC {method} response too large: {} exceeds {} bytes",
            new_len, max_bytes
        ));
    }
    buffer.extend_from_slice(chunk);
    Ok(())
}

impl SyncSource for UpstreamRpcClient {
    fn fetch_latest_block_number(&self) -> SyncSourceFuture<'_, u64> {
        Box::pin(UpstreamRpcClient::fetch_latest_block_number(self))
    }

    fn fetch_chain_id(&self) -> SyncSourceFuture<'_, String> {
        Box::pin(UpstreamRpcClient::fetch_chain_id(self))
    }

    fn fetch_block(&self, block_number: u64) -> SyncSourceFuture<'_, RuntimeFetch> {
        Box::pin(UpstreamRpcClient::fetch_block(self, block_number))
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

fn restore_storage_from_journal(
    node: &mut RuntimeNode,
    execution_state: &mut InMemoryState,
    journal: &LocalChainJournal,
    max_entries: Option<u64>,
) -> Result<u64, String> {
    let entries = journal.load_entries(max_entries)?;
    let mut restored = 0_u64;
    let mut expected = node
        .storage
        .latest_block_number()
        .map_err(|error| format!("failed to read storage tip while replaying journal: {error}"))?
        .saturating_add(1);
    for (idx, entry) in entries.into_iter().enumerate() {
        if entry.block.number != expected {
            return Err(format!(
                "local journal {} entry {} is non-sequential: expected block {}, found {}",
                journal.path().display(),
                idx + 1,
                expected,
                entry.block.number
            ));
        }
        node.storage
            .insert_block(entry.block, entry.state_diff.clone())
            .map_err(|error| {
                format!(
                    "failed to restore local journal {} entry {}: {error}",
                    journal.path().display(),
                    idx + 1
                )
            })?;
        apply_state_diff_to_in_memory_state(execution_state, &entry.state_diff);
        restored = restored.saturating_add(1);
        expected = expected.saturating_add(1);
    }
    Ok(restored)
}

fn apply_state_diff_to_in_memory_state(state: &mut InMemoryState, diff: &StarknetStateDiff) {
    for (contract, writes) in &diff.storage_diffs {
        for (key, value) in writes {
            state.set_storage(contract.clone(), key.clone(), *value);
        }
    }
    for (contract, nonce) in &diff.nonces {
        state.set_nonce(contract.clone(), *nonce);
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
    let parsed_sequencer = ContractAddress::parse(sequencer_address).map_err(|error| {
        format!(
            "invalid canonical replay sequencer address `{}`: {error}",
            replay.sequencer_address
        )
    })?;
    let block = StarknetBlock {
        number: local_number,
        parent_hash,
        state_root,
        timestamp: replay.timestamp,
        sequencer_address: parsed_sequencer,
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
        protocol_version: Version::new(0, 14, 2),
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
        .ok_or_else(|| format!("getBlockWithTxs missing sequencer_address: {block_with_txs}"))?;
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

fn parse_block_with_tx_hashes(
    block_with_tx_hashes: &Value,
    warnings: &mut Vec<String>,
) -> Result<RuntimeBlockHashes, String> {
    let external_block_number =
        value_as_u64(block_with_tx_hashes.get("block_number").ok_or_else(|| {
            format!("getBlockWithTxHashes missing block_number: {block_with_tx_hashes}")
        })?)
        .ok_or_else(|| {
            format!("invalid getBlockWithTxHashes block_number: {block_with_tx_hashes}")
        })?;

    let block_hash_raw = block_with_tx_hashes
        .get("block_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("getBlockWithTxHashes missing block_hash: {block_with_tx_hashes}")
        })?;
    let block_hash = StarknetFelt::from_str(block_hash_raw)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!("invalid getBlockWithTxHashes block_hash `{block_hash_raw}`: {error}")
        })?;

    let parent_hash_raw = block_with_tx_hashes
        .get("parent_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("getBlockWithTxHashes missing parent_hash: {block_with_tx_hashes}")
        })?;
    let parent_hash = StarknetFelt::from_str(parent_hash_raw)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| {
            format!("invalid getBlockWithTxHashes parent_hash `{parent_hash_raw}`: {error}")
        })?;

    let transactions = block_with_tx_hashes
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!("getBlockWithTxHashes missing transactions array: {block_with_tx_hashes}")
        })?;

    let mut transaction_hashes = Vec::with_capacity(transactions.len());
    for (idx, tx) in transactions.iter().enumerate() {
        let Some(tx_hash_raw) = tx.as_str() else {
            return Err(format!(
                "getBlockWithTxHashes transaction {idx} must be a hash string, got {tx}"
            ));
        };
        let tx_hash = StarknetFelt::from_str(tx_hash_raw)
            .map(|felt| format!("{:#x}", felt))
            .map_err(|error| {
                format!(
                    "invalid getBlockWithTxHashes tx hash `{tx_hash_raw}` at index {idx}: {error}"
                )
            })?;
        transaction_hashes.push(tx_hash);
    }

    if let Some(status_raw) = block_with_tx_hashes.get("status").and_then(Value::as_str) {
        if status_raw.eq_ignore_ascii_case("PENDING") {
            return Err(
                "getBlockWithTxHashes returned status=PENDING; daemon only replays finalized blocks"
                    .to_string(),
            );
        }
        if !status_raw.eq_ignore_ascii_case("ACCEPTED_ON_L2")
            && !status_raw.eq_ignore_ascii_case("ACCEPTED_ON_L1")
            && !status_raw.eq_ignore_ascii_case("FINALIZED")
        {
            warnings.push(format!(
                "unexpected tx-hashes block status `{status_raw}`; replay continues but should be verified"
            ));
        }
    }

    Ok(RuntimeBlockHashes {
        external_block_number,
        block_hash,
        parent_hash,
        transaction_hashes,
    })
}

fn compare_block_views(
    txs_view: &RuntimeBlockReplay,
    hashes_view: &RuntimeBlockHashes,
    warnings: &mut Vec<String>,
) {
    if hashes_view.external_block_number != txs_view.external_block_number {
        warnings.push(format!(
            "block number mismatch between getBlockWithTxs ({}) and getBlockWithTxHashes ({})",
            txs_view.external_block_number, hashes_view.external_block_number
        ));
    }
    if hashes_view.block_hash != txs_view.block_hash {
        warnings.push(format!(
            "block hash mismatch between getBlockWithTxs ({}) and getBlockWithTxHashes ({})",
            txs_view.block_hash, hashes_view.block_hash
        ));
    }
    if hashes_view.parent_hash != txs_view.parent_hash {
        warnings.push(format!(
            "parent hash mismatch between getBlockWithTxs ({}) and getBlockWithTxHashes ({})",
            txs_view.parent_hash, hashes_view.parent_hash
        ));
    }
    if hashes_view.transaction_hashes != txs_view.transaction_hashes {
        warnings.push(format!(
            "transaction hash list mismatch between getBlockWithTxs ({} txs) and getBlockWithTxHashes ({} txs)",
            txs_view.transaction_hashes.len(),
            hashes_view.transaction_hashes.len()
        ));
    }
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

fn parse_state_root_from_state_update(state_update: &Value) -> Result<String, String> {
    let new_root_raw = state_update
        .get("new_root")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("state_update is missing new_root: {state_update}"))?;
    let normalized_root = if let Some(raw) = new_root_raw.strip_prefix("0X") {
        format!("0x{raw}")
    } else {
        new_root_raw.to_string()
    };
    StarknetFelt::from_str(&normalized_root)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| format!("invalid state_update new_root `{new_root_raw}`: {error}"))
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
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
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
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use tempfile::tempdir;

    use super::*;

    #[derive(Clone)]
    struct MockSyncSource {
        chain_id: String,
        head: u64,
        blocks: Arc<Mutex<BTreeMap<u64, RuntimeFetch>>>,
    }

    impl MockSyncSource {
        fn with_blocks(chain_id: &str, head: u64, blocks: Vec<RuntimeFetch>) -> Self {
            let mut mapped = BTreeMap::new();
            for block in blocks {
                mapped.insert(block.replay.external_block_number, block);
            }
            Self {
                chain_id: chain_id.to_string(),
                head,
                blocks: Arc::new(Mutex::new(mapped)),
            }
        }
    }

    #[derive(Clone)]
    struct FlakyChainIdSyncSource {
        chain_id: String,
        chain_id_failures_remaining: Arc<Mutex<u32>>,
        chain_id_attempts: Arc<Mutex<u32>>,
    }

    impl FlakyChainIdSyncSource {
        fn new(chain_id: &str, failures: u32) -> Self {
            Self {
                chain_id: chain_id.to_string(),
                chain_id_failures_remaining: Arc::new(Mutex::new(failures)),
                chain_id_attempts: Arc::new(Mutex::new(0)),
            }
        }

        fn chain_id_attempts(&self) -> u32 {
            *self
                .chain_id_attempts
                .lock()
                .expect("chain_id_attempts lock should not be poisoned")
        }
    }

    #[derive(Clone)]
    struct SequenceChainIdSyncSource {
        chain_ids: Arc<Mutex<VecDeque<String>>>,
    }

    impl SequenceChainIdSyncSource {
        fn new(chain_ids: impl IntoIterator<Item = String>) -> Self {
            Self {
                chain_ids: Arc::new(Mutex::new(chain_ids.into_iter().collect())),
            }
        }
    }

    impl SyncSource for SequenceChainIdSyncSource {
        fn fetch_latest_block_number(&self) -> SyncSourceFuture<'_, u64> {
            Box::pin(async move { Ok(0) })
        }

        fn fetch_chain_id(&self) -> SyncSourceFuture<'_, String> {
            let chain_ids = Arc::clone(&self.chain_ids);
            Box::pin(async move {
                let mut guard = chain_ids
                    .lock()
                    .map_err(|_| "chain id sequence lock poisoned".to_string())?;
                if guard.len() > 1 {
                    return guard
                        .pop_front()
                        .ok_or_else(|| "empty chain id sequence".to_string());
                }
                guard
                    .front()
                    .cloned()
                    .ok_or_else(|| "empty chain id sequence".to_string())
            })
        }

        fn fetch_block(&self, block_number: u64) -> SyncSourceFuture<'_, RuntimeFetch> {
            Box::pin(async move {
                Err(format!(
                    "fetch_block({block_number}) should not be called in this test"
                ))
            })
        }
    }

    impl SyncSource for FlakyChainIdSyncSource {
        fn fetch_latest_block_number(&self) -> SyncSourceFuture<'_, u64> {
            Box::pin(async move { Ok(0) })
        }

        fn fetch_chain_id(&self) -> SyncSourceFuture<'_, String> {
            let remaining = Arc::clone(&self.chain_id_failures_remaining);
            let attempts = Arc::clone(&self.chain_id_attempts);
            let chain_id = self.chain_id.clone();
            Box::pin(async move {
                let mut attempts_guard = attempts
                    .lock()
                    .map_err(|_| "chain id attempts lock poisoned".to_string())?;
                *attempts_guard = attempts_guard.saturating_add(1);
                drop(attempts_guard);

                let mut remaining_guard = remaining
                    .lock()
                    .map_err(|_| "chain id remaining lock poisoned".to_string())?;
                if *remaining_guard > 0 {
                    *remaining_guard = remaining_guard.saturating_sub(1);
                    return Err("transient chain id rpc failure".to_string());
                }
                Ok(chain_id)
            })
        }

        fn fetch_block(&self, block_number: u64) -> SyncSourceFuture<'_, RuntimeFetch> {
            Box::pin(async move {
                Err(format!(
                    "fetch_block({block_number}) should not be called in this test"
                ))
            })
        }
    }

    impl SyncSource for MockSyncSource {
        fn fetch_latest_block_number(&self) -> SyncSourceFuture<'_, u64> {
            let head = self.head;
            Box::pin(async move { Ok(head) })
        }

        fn fetch_chain_id(&self) -> SyncSourceFuture<'_, String> {
            let chain_id = self.chain_id.clone();
            Box::pin(async move { Ok(chain_id) })
        }

        fn fetch_block(&self, block_number: u64) -> SyncSourceFuture<'_, RuntimeFetch> {
            let blocks = Arc::clone(&self.blocks);
            Box::pin(async move {
                blocks
                    .lock()
                    .map_err(|_| "mock block map lock poisoned".to_string())?
                    .get(&block_number)
                    .cloned()
                    .ok_or_else(|| format!("mock block {block_number} missing"))
            })
        }
    }

    struct RejectingConsensus;

    impl ConsensusBackend for RejectingConsensus {
        fn validate_block(&self, _input: &ConsensusInput) -> Result<ConsensusVerdict, String> {
            Ok(ConsensusVerdict::Reject)
        }
    }

    struct HealthyConsensus;

    impl ConsensusBackend for HealthyConsensus {
        fn validate_block(&self, _input: &ConsensusInput) -> Result<ConsensusVerdict, String> {
            Ok(ConsensusVerdict::Accept)
        }
    }

    struct MockNetwork {
        healthy: bool,
        peers: usize,
    }

    impl NetworkBackend for MockNetwork {
        fn is_healthy(&self) -> bool {
            self.healthy
        }

        fn peer_count(&self) -> usize {
            self.peers
        }
    }

    fn runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            chain_id: ChainId::Mainnet,
            upstream_rpc_url: "http://localhost:9545".to_string(),
            replay_window: 1,
            max_replay_per_poll: 16,
            replay_checkpoint_path: None,
            poll_interval: Duration::from_millis(500),
            rpc_timeout: Duration::from_secs(3),
            retry: RpcRetryConfig::default(),
            peer_count_hint: 0,
            require_peers: false,
            local_journal_path: None,
        }
    }

    fn sample_fetch(
        external_block_number: u64,
        parent_hash: &str,
        block_hash: &str,
    ) -> RuntimeFetch {
        RuntimeFetch {
            replay: RuntimeBlockReplay {
                external_block_number,
                block_hash: block_hash.to_string(),
                parent_hash: parent_hash.to_string(),
                sequencer_address: "0x1".to_string(),
                timestamp: 1_700_000_000 + external_block_number,
                transaction_hashes: vec!["0x111".to_string()],
            },
            state_diff: StarknetStateDiff::default(),
            state_root: "0x10".to_string(),
        }
    }

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
    fn parse_block_with_txs_requires_sequencer_address() {
        let block = json!({
            "block_number": 12,
            "block_hash": "0xabc",
            "parent_hash": "0x123",
            "timestamp": 1_700_000_012_u64,
            "transactions": [{"transaction_hash": "0x1"}]
        });
        let mut warnings = Vec::new();
        let error = parse_block_with_txs(&block, &mut warnings)
            .expect_err("missing sequencer_address must fail closed");
        assert!(error.contains("missing sequencer_address"));
    }

    #[test]
    fn parse_block_with_tx_hashes_parses_hash_array() {
        let block = json!({
            "block_number": 12,
            "block_hash": "0xabc",
            "parent_hash": "0x123",
            "transactions": ["0x1", "0x2"],
            "status": "ACCEPTED_ON_L2"
        });

        let mut warnings = Vec::new();
        let parsed = parse_block_with_tx_hashes(&block, &mut warnings).expect("must parse");
        assert_eq!(parsed.external_block_number, 12);
        assert_eq!(parsed.block_hash, "0xabc");
        assert_eq!(parsed.parent_hash, "0x123");
        assert_eq!(parsed.transaction_hashes, vec!["0x1", "0x2"]);
        assert!(warnings.is_empty());
    }

    #[test]
    fn parse_block_with_tx_hashes_requires_transactions_array() {
        let block = json!({
            "block_number": 12,
            "block_hash": "0xabc",
            "parent_hash": "0x123",
            "status": "ACCEPTED_ON_L2"
        });

        let mut warnings = Vec::new();
        let error = parse_block_with_tx_hashes(&block, &mut warnings)
            .expect_err("missing transactions array must fail closed");
        assert!(error.contains("missing transactions array"));
    }

    #[test]
    fn compare_block_views_detects_mismatch() {
        let txs_view = RuntimeBlockReplay {
            external_block_number: 7,
            block_hash: "0xaaa".to_string(),
            parent_hash: "0xbbb".to_string(),
            sequencer_address: "0x1".to_string(),
            timestamp: 1_700_000_007,
            transaction_hashes: vec!["0x1".to_string(), "0x2".to_string()],
        };
        let hashes_view = RuntimeBlockHashes {
            external_block_number: 7,
            block_hash: "0xccc".to_string(),
            parent_hash: "0xbbb".to_string(),
            transaction_hashes: vec!["0x1".to_string()],
        };

        let mut warnings = Vec::new();
        compare_block_views(&txs_view, &hashes_view, &mut warnings);
        assert_eq!(warnings.len(), 2);
        assert!(warnings[0].contains("block hash mismatch"));
        assert!(warnings[1].contains("transaction hash list mismatch"));
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
    fn parse_state_root_requires_new_root() {
        let state_update = json!({
            "state_diff": {}
        });
        let error = parse_state_root_from_state_update(&state_update)
            .expect_err("missing new_root must fail closed");
        assert!(error.contains("missing new_root"));
    }

    #[test]
    fn parse_state_root_canonicalizes_hex() {
        let state_update = json!({
            "new_root": "0X000A",
            "state_diff": {}
        });
        let root =
            parse_state_root_from_state_update(&state_update).expect("valid new_root should parse");
        assert_eq!(root, "0xa");
    }

    #[test]
    fn append_limited_chunk_rejects_oversized_payload() {
        let mut buffer = vec![1, 2, 3];
        let error = append_limited_chunk(&mut buffer, &[4, 5, 6], 5, "starknet_test")
            .expect_err("chunk growth beyond limit must fail");
        assert!(error.contains("response too large"));
    }

    #[test]
    fn normalize_chain_id_accepts_hex_encoded_ascii() {
        assert_eq!(normalize_chain_id("SN_MAIN"), "SN_MAIN");
        assert_eq!(normalize_chain_id("0x534e5f4d41494e"), "SN_MAIN");
    }

    #[tokio::test]
    async fn poll_once_fails_closed_when_network_is_unhealthy() {
        let sync_source = Arc::new(MockSyncSource::with_blocks("SN_MAIN", 0, Vec::new()));
        let consensus = Arc::new(HealthyConsensus);
        let network = Arc::new(MockNetwork {
            healthy: false,
            peers: 0,
        });

        let mut runtime =
            NodeRuntime::new_with_backends(runtime_config(), sync_source, consensus, network)
                .expect("runtime should initialize");

        let error = runtime
            .poll_once()
            .await
            .expect_err("unhealthy network must fail closed");
        assert!(error.contains("network backend unhealthy"));
        assert_eq!(runtime.diagnostics().network_failures, 1);
    }

    #[tokio::test]
    async fn poll_once_rejects_blocks_when_consensus_rejects() {
        let sync_source = Arc::new(MockSyncSource::with_blocks(
            "SN_MAIN",
            1,
            vec![sample_fetch(1, "0x0", "0x1")],
        ));
        let consensus = Arc::new(RejectingConsensus);
        let network = Arc::new(MockNetwork {
            healthy: true,
            peers: 5,
        });

        let mut runtime =
            NodeRuntime::new_with_backends(runtime_config(), sync_source, consensus, network)
                .expect("runtime should initialize");

        let error = runtime
            .poll_once()
            .await
            .expect_err("consensus rejection must fail closed");
        assert!(error.contains("consensus rejected external block 1"));
        assert_eq!(runtime.diagnostics().consensus_rejections, 1);
        assert_eq!(
            runtime
                .storage()
                .latest_block_number()
                .expect("storage read should work"),
            0
        );
    }

    #[tokio::test]
    async fn runtime_restores_local_chain_from_journal_on_restart() {
        let dir = tempdir().expect("tempdir");
        let journal_path = dir.path().join("local-journal.jsonl");
        let checkpoint_path = dir.path().join("replay-checkpoint.json");

        let mut config = runtime_config();
        config.local_journal_path = Some(journal_path.display().to_string());
        config.replay_checkpoint_path = Some(checkpoint_path.display().to_string());

        let consensus = Arc::new(HealthyConsensus);
        let network = Arc::new(MockNetwork {
            healthy: true,
            peers: 3,
        });
        let mut runtime = NodeRuntime::new_with_backends(
            config.clone(),
            Arc::new(MockSyncSource::with_blocks(
                "SN_MAIN",
                1,
                vec![sample_fetch(1, "0x0", "0x1")],
            )),
            consensus.clone(),
            network.clone(),
        )
        .expect("runtime should initialize");
        runtime.poll_once().await.expect("first poll should commit");
        assert_eq!(
            runtime
                .storage()
                .latest_block_number()
                .expect("storage read should work"),
            1
        );

        let restarted = NodeRuntime::new_with_backends(
            config,
            Arc::new(MockSyncSource::with_blocks("SN_MAIN", 1, Vec::new())),
            consensus,
            network,
        )
        .expect("runtime should restore from journal");
        assert_eq!(
            restarted
                .storage()
                .latest_block_number()
                .expect("restored storage read should work"),
            1
        );
        let progress = restarted
            .sync_progress_handle()
            .lock()
            .expect("sync progress lock should not be poisoned")
            .clone();
        assert_eq!(progress.starting_block, 1);
        assert_eq!(progress.current_block, 1);
    }

    #[tokio::test]
    async fn runtime_ignores_journal_entries_beyond_checkpoint_cursor() {
        let dir = tempdir().expect("tempdir");
        let journal_path = dir.path().join("local-journal.jsonl");
        let checkpoint_path = dir.path().join("replay-checkpoint.json");

        let mut config = runtime_config();
        config.local_journal_path = Some(journal_path.display().to_string());
        config.replay_checkpoint_path = Some(checkpoint_path.display().to_string());

        let consensus = Arc::new(HealthyConsensus);
        let network = Arc::new(MockNetwork {
            healthy: true,
            peers: 3,
        });
        let mut runtime = NodeRuntime::new_with_backends(
            config.clone(),
            Arc::new(MockSyncSource::with_blocks(
                "SN_MAIN",
                1,
                vec![sample_fetch(1, "0x0", "0x1")],
            )),
            consensus.clone(),
            network.clone(),
        )
        .expect("runtime should initialize");
        runtime.poll_once().await.expect("first poll should commit");

        let extra_fetch = sample_fetch(2, "0x1", "0x2");
        let extra_block = ingest_block_from_fetch(2, &extra_fetch).expect("build extra block");
        LocalChainJournal::new(&journal_path)
            .append_entry(&LocalJournalEntry {
                block: extra_block,
                state_diff: extra_fetch.state_diff,
            })
            .expect("append extra journal entry");

        let restarted = NodeRuntime::new_with_backends(
            config,
            Arc::new(MockSyncSource::with_blocks("SN_MAIN", 1, Vec::new())),
            consensus,
            network,
        )
        .expect("runtime should restore from checkpoint-aligned journal prefix");
        assert_eq!(
            restarted
                .storage()
                .latest_block_number()
                .expect("restored storage read should work"),
            1
        );
    }

    #[test]
    fn local_journal_rejects_oversized_files() {
        let dir = tempdir().expect("tempdir");
        let journal_path = dir.path().join("local-journal.jsonl");
        let file = std::fs::File::create(&journal_path).expect("create journal file");
        file.set_len(MAX_LOCAL_JOURNAL_FILE_BYTES.saturating_add(1))
            .expect("expand file");

        let journal = LocalChainJournal::new(&journal_path);
        let error = journal
            .load_entries(None)
            .expect_err("oversized journal must fail closed");
        assert!(error.contains("exceeds max allowed"));
    }

    #[tokio::test]
    async fn chain_id_validation_retries_transient_upstream_errors() {
        let sync_source = Arc::new(FlakyChainIdSyncSource::new("SN_MAIN", 1));
        let consensus = Arc::new(HealthyConsensus);
        let network = Arc::new(MockNetwork {
            healthy: true,
            peers: 2,
        });

        let mut config = runtime_config();
        config.retry = RpcRetryConfig {
            max_retries: 2,
            base_backoff: Duration::ZERO,
        };

        let mut runtime =
            NodeRuntime::new_with_backends(config, sync_source.clone(), consensus, network)
                .expect("runtime should initialize");
        runtime
            .ensure_chain_id_validated()
            .await
            .expect("transient chain-id failure should recover with retry");

        assert_eq!(sync_source.chain_id_attempts(), 2);
        assert_eq!(runtime.diagnostics().failure_count, 0);
        assert!(runtime.chain_id_validated);
    }

    #[tokio::test]
    async fn chain_id_validation_revalidates_after_initial_success() {
        let sync_source = Arc::new(SequenceChainIdSyncSource::new([
            "SN_MAIN".to_string(),
            "SN_SEPOLIA".to_string(),
        ]));
        let consensus = Arc::new(HealthyConsensus);
        let network = Arc::new(MockNetwork {
            healthy: true,
            peers: 2,
        });

        let mut config = runtime_config();
        config.retry = RpcRetryConfig {
            max_retries: 0,
            base_backoff: Duration::ZERO,
        };

        let mut runtime = NodeRuntime::new_with_backends(config, sync_source, consensus, network)
            .expect("runtime should initialize");
        runtime
            .ensure_chain_id_validated()
            .await
            .expect("initial validation should succeed");

        let error = runtime
            .ensure_chain_id_validated()
            .await
            .expect_err("second validation must detect upstream chain id switch");
        assert!(error.contains("chain id mismatch"));
    }
}
