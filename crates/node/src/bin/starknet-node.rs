use std::collections::{BTreeMap, HashSet, VecDeque};
use std::env;
use std::future::Future;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::time::interval;

use starknet_node::ChainId;
use starknet_node::runtime::{
    DEFAULT_CHAIN_ID_REVALIDATE_POLLS, DEFAULT_MAX_REPLAY_PER_POLL, DEFAULT_REPLAY_WINDOW,
    DEFAULT_RPC_MAX_RETRIES, DEFAULT_RPC_RETRY_BACKOFF_MS, DEFAULT_RPC_TIMEOUT_SECS,
    DEFAULT_STORAGE_SNAPSHOT_INTERVAL_BLOCKS, DEFAULT_SYNC_POLL_MS, NodeRuntime, RpcRetryConfig,
    RuntimeConfig, SyncProgress,
};
use starknet_node_rpc::{StarknetRpcServer, SyncStatus};
use starknet_node_storage::{InMemoryStorage, ThreadSafeStorage};

const DEFAULT_RPC_BIND: &str = "127.0.0.1:9545";
const DEFAULT_REPLAY_CHECKPOINT_PATH: &str = ".pastis/node-replay-checkpoint.json";
const DEFAULT_LOCAL_JOURNAL_PATH: &str = ".pastis/node-local-journal.jsonl";
const DEFAULT_STORAGE_SNAPSHOT_PATH: &str = ".pastis/node-storage.snapshot";
const DEFAULT_P2P_HEARTBEAT_MS: u64 = 30_000;
const MIN_NETWORK_STALE_AFTER_MS: u64 = 30_000;
const MAX_NETWORK_STALE_AFTER_MS: u64 = 600_000;
const DEFAULT_RPC_MAX_CONCURRENCY: usize = 256;
const DEFAULT_RPC_RATE_LIMIT_PER_MINUTE: u32 = 1_200;
const MAX_RPC_RATE_LIMIT_PER_MINUTE: u32 = 100_000;
const MAX_UPSTREAM_RPC_ENDPOINTS: usize = 16;
const MAX_UPSTREAM_RPC_URL_BYTES: usize = 4 * 1024;
const MAX_BOOTNODE_PROBE_CONCURRENCY: usize = 32;
const MAX_BOOTNODES: usize = 1_024;
const MAX_BOOTNODE_ENTRY_BYTES: usize = 1_024;
const DEFAULT_HEALTH_MAX_CONSECUTIVE_FAILURES: u64 = 3;
const DEFAULT_HEALTH_MAX_SYNC_LAG_BLOCKS: u64 = 64;
const DAEMON_GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 5;
const MAX_RPC_AUTH_TOKEN_BYTES: usize = 4 * 1024;
const MAX_TRACKED_RPC_CLIENTS: usize = 10_000;
const DEFAULT_PEER_HEALTH_HISTORY_CAPACITY: usize = 512;
const DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS: u64 = 300;
const DEFAULT_PEER_HEALTH_MAX_RECENT_FLAPS: u64 = 0;

#[derive(Debug, Clone)]
struct DaemonConfig {
    upstream_rpc_url: String,
    rpc_bind: String,
    rpc_auth_token: Option<String>,
    allow_public_rpc_bind: bool,
    chain_id: ChainId,
    poll_ms: u64,
    replay_window: u64,
    max_replay_per_poll: u64,
    chain_id_revalidate_polls: u64,
    replay_checkpoint_path: Option<String>,
    local_journal_path: Option<String>,
    storage_snapshot_path: Option<String>,
    storage_snapshot_interval_blocks: u64,
    rpc_timeout_secs: u64,
    rpc_max_retries: u32,
    rpc_retry_backoff_ms: u64,
    rpc_max_concurrency: usize,
    rpc_rate_limit_per_minute: u32,
    disable_batch_requests: bool,
    strict_canonical_execution: bool,
    bootnodes: Vec<String>,
    min_peers: u64,
    p2p_heartbeat_ms: u64,
    peer_health_history_capacity: usize,
    peer_health_flap_window_secs: u64,
    peer_health_max_recent_flaps: u64,
    health_max_consecutive_failures: u64,
    health_max_sync_lag_blocks: u64,
    exit_on_unhealthy: bool,
}

#[derive(Clone)]
struct RpcAppState {
    storage: ThreadSafeStorage<InMemoryStorage>,
    chain_id: String,
    rpc_auth_token: Option<String>,
    sync_progress: Arc<Mutex<SyncProgress>>,
    health_policy: HealthPolicy,
    min_peer_count: u64,
    max_recent_peer_health_flaps: u64,
    peer_health_tracker: Arc<Mutex<PeerHealthTracker>>,
    rpc_slots: Arc<Semaphore>,
    rpc_rate_limiter: Arc<Mutex<RpcRateLimiter>>,
    rpc_metrics: Arc<Mutex<RpcRuntimeMetrics>>,
}

#[derive(Debug, Clone, Serialize)]
struct StatusPayload {
    chain_id: String,
    starting_block: u64,
    current_block: u64,
    highest_block: u64,
    peer_count: u64,
    min_peers: u64,
    max_recent_peer_health_flaps: u64,
    reorg_events: u64,
    consecutive_failures: u64,
    last_error: Option<String>,
    peer_health: PeerHealthStatus,
}

#[derive(Debug, Clone)]
struct HealthPolicy {
    max_consecutive_failures: u64,
    max_sync_lag: u64,
    require_peers: bool,
}

#[derive(Debug, Clone, Serialize)]
struct PeerHealthStatus {
    healthy: bool,
    last_peer_count: Option<u64>,
    history_size: usize,
    observations_total: u64,
    healthy_observations_total: u64,
    unhealthy_observations_total: u64,
    flap_transitions_total: u64,
    flap_transitions_recent: u64,
    last_observed_at_unix_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
struct PeerHealthSample {
    observed_at_unix_seconds: u64,
    peer_count: u64,
    healthy: bool,
}

#[derive(Debug)]
struct PeerHealthTracker {
    min_peer_count: u64,
    history_capacity: usize,
    flap_window_secs: u64,
    samples: VecDeque<PeerHealthSample>,
    observations_total: u64,
    healthy_observations_total: u64,
    unhealthy_observations_total: u64,
    flap_transitions_total: u64,
    flap_transition_timestamps: VecDeque<u64>,
    last_health: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum BootnodeEndpoint {
    Socket(SocketAddr),
    HostPort { host: String, port: u16 },
}

#[derive(Debug, Default)]
struct RpcRateLimiter {
    limit_per_minute: u32,
    requests_by_ip: BTreeMap<IpAddr, VecDeque<u64>>,
}

impl RpcRateLimiter {
    fn new(limit_per_minute: u32) -> Self {
        Self {
            limit_per_minute,
            requests_by_ip: BTreeMap::new(),
        }
    }

    fn check_and_record(&mut self, ip: IpAddr, now_unix_seconds: u64) -> Result<(), String> {
        if self.limit_per_minute == 0 {
            return Ok(());
        }

        let requests = self.requests_by_ip.entry(ip).or_default();
        while let Some(ts) = requests.front() {
            if now_unix_seconds.saturating_sub(*ts) < 60 {
                break;
            }
            requests.pop_front();
        }
        if requests.len() as u32 >= self.limit_per_minute {
            return Err(format!(
                "rpc rate limit exceeded for {ip}: limit {} requests/minute",
                self.limit_per_minute
            ));
        }
        requests.push_back(now_unix_seconds);
        self.enforce_capacity(now_unix_seconds, ip);
        Ok(())
    }

    fn enforce_capacity(&mut self, now_unix_seconds: u64, current_ip: IpAddr) {
        if self.requests_by_ip.len() <= MAX_TRACKED_RPC_CLIENTS {
            return;
        }

        for requests in self.requests_by_ip.values_mut() {
            while let Some(ts) = requests.front() {
                if now_unix_seconds.saturating_sub(*ts) < 60 {
                    break;
                }
                requests.pop_front();
            }
        }
        self.requests_by_ip
            .retain(|_, requests| !requests.is_empty());

        while self.requests_by_ip.len() > MAX_TRACKED_RPC_CLIENTS {
            let evict = self
                .requests_by_ip
                .keys()
                .copied()
                .find(|ip| *ip != current_ip)
                .or_else(|| self.requests_by_ip.keys().copied().next());
            let Some(evict) = evict else {
                break;
            };
            self.requests_by_ip.remove(&evict);
        }
    }
}

#[derive(Debug, Default)]
struct RpcRuntimeMetrics {
    requests_total: u64,
    responses_ok_total: u64,
    responses_no_content_total: u64,
    unauthorized_total: u64,
    rate_limited_total: u64,
    concurrency_rejected_total: u64,
    internal_error_total: u64,
}

#[derive(Debug)]
struct BootnodeObservation {
    peer_count: AtomicU64,
    observed_at_unix_seconds: AtomicU64,
}

impl BootnodeObservation {
    fn new(peer_count: u64, observed_at_unix_seconds: u64) -> Self {
        Self {
            peer_count: AtomicU64::new(peer_count),
            observed_at_unix_seconds: AtomicU64::new(observed_at_unix_seconds),
        }
    }

    fn update(&self, peer_count: u64, observed_at_unix_seconds: u64) {
        self.peer_count.store(peer_count, Ordering::Relaxed);
        self.observed_at_unix_seconds
            .store(observed_at_unix_seconds, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.peer_count.load(Ordering::Relaxed),
            self.observed_at_unix_seconds.load(Ordering::Relaxed),
        )
    }
}

impl PeerHealthTracker {
    fn new(min_peer_count: u64, history_capacity: usize, flap_window_secs: u64) -> Self {
        Self {
            min_peer_count,
            history_capacity,
            flap_window_secs,
            samples: VecDeque::with_capacity(history_capacity),
            observations_total: 0,
            healthy_observations_total: 0,
            unhealthy_observations_total: 0,
            flap_transitions_total: 0,
            flap_transition_timestamps: VecDeque::new(),
            last_health: None,
        }
    }

    fn record(&mut self, peer_count: u64, observed_at_unix_seconds: u64) {
        let healthy = peer_meets_minimum(peer_count, self.min_peer_count);
        self.observations_total = self.observations_total.saturating_add(1);
        if healthy {
            self.healthy_observations_total = self.healthy_observations_total.saturating_add(1);
        } else {
            self.unhealthy_observations_total = self.unhealthy_observations_total.saturating_add(1);
        }

        if let Some(last_health) = self.last_health
            && last_health != healthy
        {
            self.flap_transitions_total = self.flap_transitions_total.saturating_add(1);
            self.flap_transition_timestamps
                .push_back(observed_at_unix_seconds);
        }
        self.last_health = Some(healthy);

        self.samples.push_back(PeerHealthSample {
            observed_at_unix_seconds,
            peer_count,
            healthy,
        });
        while self.samples.len() > self.history_capacity {
            self.samples.pop_front();
        }
        self.prune_flap_transitions(observed_at_unix_seconds);
    }

    fn snapshot(&self, now_unix_seconds: u64) -> PeerHealthStatus {
        let flap_transitions_recent = self
            .flap_transition_timestamps
            .iter()
            .filter(|ts| now_unix_seconds.saturating_sub(**ts) <= self.flap_window_secs)
            .count() as u64;
        let (healthy, last_observed_at_unix_seconds, last_peer_count) = self
            .samples
            .back()
            .map(|sample| {
                (
                    sample.healthy,
                    Some(sample.observed_at_unix_seconds),
                    Some(sample.peer_count),
                )
            })
            .unwrap_or((peer_meets_minimum(0, self.min_peer_count), None, None));

        PeerHealthStatus {
            healthy,
            last_peer_count,
            history_size: self.samples.len(),
            observations_total: self.observations_total,
            healthy_observations_total: self.healthy_observations_total,
            unhealthy_observations_total: self.unhealthy_observations_total,
            flap_transitions_total: self.flap_transitions_total,
            flap_transitions_recent,
            last_observed_at_unix_seconds,
        }
    }

    fn prune_flap_transitions(&mut self, now_unix_seconds: u64) {
        while let Some(ts) = self.flap_transition_timestamps.front() {
            if now_unix_seconds.saturating_sub(*ts) <= self.flap_window_secs {
                break;
            }
            self.flap_transition_timestamps.pop_front();
        }
    }
}

fn peer_meets_minimum(peer_count: u64, min_peer_count: u64) -> bool {
    if min_peer_count == 0 {
        true
    } else {
        peer_count >= min_peer_count
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let config = parse_daemon_config()?;
    let bootnode_endpoints = parse_bootnode_endpoints(&config.bootnodes)?;
    validate_peer_requirements(config.min_peers, bootnode_endpoints.len())?;
    let initial_peers = if bootnode_endpoints.is_empty() {
        0
    } else {
        probe_bootnodes(&bootnode_endpoints, Duration::from_millis(1_500)).await
    };

    let runtime_config = RuntimeConfig {
        chain_id: config.chain_id.clone(),
        upstream_rpc_url: config.upstream_rpc_url.clone(),
        replay_window: config.replay_window,
        max_replay_per_poll: config.max_replay_per_poll,
        chain_id_revalidate_polls: config.chain_id_revalidate_polls,
        replay_checkpoint_path: config.replay_checkpoint_path.clone(),
        delete_checkpoints_on_zero_tip: false,
        local_journal_path: config.local_journal_path.clone(),
        storage_snapshot_path: config.storage_snapshot_path.clone(),
        storage_snapshot_interval_blocks: config.storage_snapshot_interval_blocks,
        poll_interval: Duration::from_millis(config.poll_ms),
        rpc_timeout: Duration::from_secs(config.rpc_timeout_secs),
        retry: RpcRetryConfig {
            max_retries: config.rpc_max_retries,
            base_backoff: Duration::from_millis(config.rpc_retry_backoff_ms),
        },
        disable_batch_requests: config.disable_batch_requests,
        network_stale_after: derive_network_stale_after(config.p2p_heartbeat_ms),
        strict_canonical_execution: config.strict_canonical_execution,
        peer_count_hint: initial_peers,
        require_peers: config.min_peers > 0,
        storage: None,
    };

    let initial_observed_at = unix_now_seconds();
    let bootnode_observation = Arc::new(BootnodeObservation::new(
        initial_peers as u64,
        initial_observed_at,
    ));
    let peer_health_tracker = Arc::new(Mutex::new(PeerHealthTracker::new(
        config.min_peers,
        config.peer_health_history_capacity,
        config.peer_health_flap_window_secs,
    )));
    if let Ok(mut tracker) = peer_health_tracker.lock() {
        tracker.record(initial_peers as u64, initial_observed_at);
    }

    let mut runtime = NodeRuntime::new(runtime_config)?;
    runtime.report_peer_observation(initial_peers, initial_observed_at);
    let storage = runtime.storage();
    let sync_progress = runtime.sync_progress_handle();
    let chain_id = runtime.chain_id().to_string();

    let app_state = RpcAppState {
        storage,
        chain_id: chain_id.clone(),
        rpc_auth_token: config.rpc_auth_token.clone(),
        sync_progress,
        health_policy: HealthPolicy {
            max_consecutive_failures: config.health_max_consecutive_failures,
            max_sync_lag: config.health_max_sync_lag_blocks,
            require_peers: config.min_peers > 0,
        },
        min_peer_count: config.min_peers,
        max_recent_peer_health_flaps: config.peer_health_max_recent_flaps,
        peer_health_tracker: Arc::clone(&peer_health_tracker),
        rpc_slots: Arc::new(Semaphore::new(config.rpc_max_concurrency)),
        rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(
            config.rpc_rate_limit_per_minute,
        ))),
        rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
    };

    if let Ok(mut progress) = app_state.sync_progress.lock() {
        progress.peer_count = initial_peers as u64;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut heartbeat_handle = if !bootnode_endpoints.is_empty() {
        let endpoints = bootnode_endpoints.clone();
        let bootnode_count = endpoints.len();
        let heartbeat_ms = config.p2p_heartbeat_ms.max(1_000);
        let observation = Arc::clone(&bootnode_observation);
        let sync_progress = app_state.sync_progress.clone();
        let peer_health_tracker = Arc::clone(&peer_health_tracker);
        let heartbeat_shutdown_rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            run_bootnode_heartbeat(
                endpoints,
                bootnode_count,
                heartbeat_ms,
                observation,
                sync_progress,
                peer_health_tracker,
                heartbeat_shutdown_rx,
            )
            .await;
        }))
    } else {
        None
    };

    let app = Router::new()
        .route("/", post(handle_rpc))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(app_state);

    let listener = TcpListener::bind(&config.rpc_bind)
        .await
        .map_err(|error| format!("failed to bind RPC endpoint {}: {error}", config.rpc_bind))?;

    println!("starknet-node daemon starting");
    println!("chain_id: {chain_id}");
    println!(
        "upstream_rpc_urls: {}",
        redact_rpc_urls(&config.upstream_rpc_url)
    );
    println!("rpc_bind: {}", config.rpc_bind);
    println!("poll_ms: {}", config.poll_ms);
    println!(
        "chain_id_revalidate_polls: {}",
        config.chain_id_revalidate_polls
    );
    println!("rpc_max_concurrency: {}", config.rpc_max_concurrency);
    println!(
        "rpc_rate_limit_per_minute: {}",
        config.rpc_rate_limit_per_minute
    );
    println!("disable_batch_requests: {}", config.disable_batch_requests);
    println!(
        "strict_canonical_execution: {}",
        config.strict_canonical_execution
    );
    println!("rpc_auth_enabled: {}", config.rpc_auth_token.is_some());
    println!("allow_public_rpc_bind: {}", config.allow_public_rpc_bind);
    println!("exit_on_unhealthy: {}", config.exit_on_unhealthy);
    println!("min_peers: {}", config.min_peers);
    println!(
        "peer_health_history_capacity: {}",
        config.peer_health_history_capacity
    );
    println!(
        "peer_health_flap_window_secs: {}",
        config.peer_health_flap_window_secs
    );
    println!(
        "peer_health_max_recent_flaps: {}",
        config.peer_health_max_recent_flaps
    );
    if let Some(path) = &config.local_journal_path {
        println!("local_journal_path: {path}");
    }
    if let Some(path) = &config.storage_snapshot_path {
        println!("storage_snapshot_path: {path}");
        println!(
            "storage_snapshot_interval_blocks: {}",
            config.storage_snapshot_interval_blocks
        );
    }
    println!("require_peers: {}", config.min_peers > 0);

    let mut rpc_shutdown_rx = shutdown_rx.clone();
    let mut rpc_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            wait_for_shutdown_signal(&mut rpc_shutdown_rx).await;
        })
        .await
        .map_err(|error| format!("rpc server failed: {error}"))
    });

    if let Err(error) = runtime.poll_once().await {
        eprintln!("warning: initial sync poll failed: {error}");
    }

    let mut ticker = new_daemon_interval(runtime.poll_interval());
    ticker.tick().await;

    let mut exit_error: Option<String> = None;
    let mut rpc_outcome: Option<Result<Result<(), String>, tokio::task::JoinError>> = None;
    let mut rpc_exited_before_shutdown = false;

    loop {
        tokio::select! {
            rpc_join = &mut rpc_handle => {
                rpc_exited_before_shutdown = true;
                rpc_outcome = Some(rpc_join);
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("received shutdown signal");
                break;
            }
            _ = ticker.tick() => {
                let (observed_peers, observed_at) = bootnode_observation.snapshot();
                runtime.report_peer_observation(observed_peers as usize, observed_at);
                if let Err(error) = runtime.poll_once().await {
                    eprintln!("warning: sync poll failed: {error}");
                }
                let progress = {
                    let progress_handle = runtime.sync_progress_handle();
                    let mut progress = progress_handle
                        .lock()
                        .map_err(|_| "sync progress lock poisoned".to_string())?;
                    progress.peer_count = observed_peers;
                    progress.clone()
                };
                if let Some(reason) = unhealthy_exit_reason(
                    &progress,
                    &HealthPolicy {
                        max_consecutive_failures: config.health_max_consecutive_failures,
                        max_sync_lag: config.health_max_sync_lag_blocks,
                        require_peers: config.min_peers > 0,
                    },
                    config.min_peers,
                    config.exit_on_unhealthy,
                    peer_health_tracker
                        .lock()
                        .map_err(|_| "peer health tracker lock poisoned".to_string())?
                        .snapshot(unix_now_seconds()),
                    config.peer_health_max_recent_flaps,
                ) {
                    exit_error = Some(format!(
                        "fatal health condition while sync loop is active: {reason}"
                    ));
                    break;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);

    if rpc_outcome.is_none() {
        match join_task_with_timeout(
            &mut rpc_handle,
            Duration::from_secs(DAEMON_GRACEFUL_SHUTDOWN_TIMEOUT_SECS),
            "rpc server",
        )
        .await
        {
            Ok(outcome) => {
                rpc_outcome = Some(outcome);
            }
            Err(error) => {
                if exit_error.is_none() {
                    exit_error = Some(error);
                } else {
                    eprintln!("warning: {error}");
                }
            }
        }
    }

    if let Some(mut heartbeat_handle) = heartbeat_handle.take() {
        match join_task_with_timeout(
            &mut heartbeat_handle,
            Duration::from_secs(DAEMON_GRACEFUL_SHUTDOWN_TIMEOUT_SECS),
            "bootnode heartbeat",
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let warning = format!("bootnode heartbeat task join error: {error}");
                if exit_error.is_none() {
                    exit_error = Some(warning.clone());
                } else {
                    eprintln!("warning: {warning}");
                }
            }
            Err(error) => {
                if exit_error.is_none() {
                    exit_error = Some(error);
                } else {
                    eprintln!("warning: {error}");
                }
            }
        }
    }

    if let Some(outcome) = rpc_outcome
        && let Err(error) = classify_rpc_task_completion(outcome, !rpc_exited_before_shutdown)
    {
        if exit_error.is_none() {
            exit_error = Some(error);
        } else {
            eprintln!("warning: {error}");
        }
    }

    if let Some(error) = exit_error {
        return Err(error);
    }
    Ok(())
}

async fn handle_rpc(
    State(state): State<RpcAppState>,
    headers: HeaderMap,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    raw: String,
) -> impl IntoResponse {
    increment_rpc_metrics(&state, |metrics| {
        metrics.requests_total = metrics.requests_total.saturating_add(1);
    });

    if !is_rpc_request_authorized(&headers, state.rpc_auth_token.as_deref()) {
        increment_rpc_metrics(&state, |metrics| {
            metrics.unauthorized_total = metrics.unauthorized_total.saturating_add(1);
        });
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "missing or invalid bearer token".to_string(),
        )
            .into_response();
    }

    let now_unix_seconds = unix_now_seconds();
    {
        let mut limiter = match state.rpc_rate_limiter.lock() {
            Ok(limiter) => limiter,
            Err(_) => {
                increment_rpc_metrics(&state, |metrics| {
                    metrics.internal_error_total = metrics.internal_error_total.saturating_add(1);
                });
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "rpc rate limiter lock poisoned".to_string(),
                )
                    .into_response();
            }
        };
        if let Err(message) = limiter.check_and_record(peer_addr.ip(), now_unix_seconds) {
            increment_rpc_metrics(&state, |metrics| {
                metrics.rate_limited_total = metrics.rate_limited_total.saturating_add(1);
            });
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "60")],
                message,
            )
                .into_response();
        }
    }

    let _rpc_slot = match state.rpc_slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            increment_rpc_metrics(&state, |metrics| {
                metrics.concurrency_rejected_total =
                    metrics.concurrency_rejected_total.saturating_add(1);
            });
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "rpc concurrency limit reached".to_string(),
            )
                .into_response();
        }
    };

    let sync_status = match state.sync_progress.lock() {
        Ok(progress) => SyncStatus {
            starting_block_num: progress.starting_block,
            current_block_num: progress.current_block,
            highest_block_num: progress.highest_block,
        },
        Err(_) => {
            increment_rpc_metrics(&state, |metrics| {
                metrics.internal_error_total = metrics.internal_error_total.saturating_add(1);
            });
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "sync progress lock poisoned".to_string(),
            )
                .into_response();
        }
    };

    let response = StarknetRpcServer::new(&state.storage, state.chain_id.clone())
        .with_sync_status(sync_status)
        .handle_raw(&raw);

    if response.is_empty() {
        increment_rpc_metrics(&state, |metrics| {
            metrics.responses_no_content_total =
                metrics.responses_no_content_total.saturating_add(1);
        });
        return StatusCode::NO_CONTENT.into_response();
    }

    increment_rpc_metrics(&state, |metrics| {
        metrics.responses_ok_total = metrics.responses_ok_total.saturating_add(1);
    });
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        response,
    )
        .into_response()
}

fn increment_rpc_metrics(state: &RpcAppState, update: impl FnOnce(&mut RpcRuntimeMetrics)) {
    if let Ok(mut metrics) = state.rpc_metrics.lock() {
        update(&mut metrics);
    }
}

fn is_rpc_request_authorized(headers: &HeaderMap, required_token: Option<&str>) -> bool {
    let Some(required) = required_token else {
        return true;
    };
    let Some(raw_auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    let Some((scheme, token)) = raw_auth.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("bearer") {
        return false;
    }
    constant_time_eq_str(token, required)
}

fn constant_time_eq_str(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut diff = left.len() ^ right.len();
    let max_len = left.len().max(right.len());
    for i in 0..max_len {
        let lhs = left.get(i).copied().unwrap_or_default();
        let rhs = right.get(i).copied().unwrap_or_default();
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

fn unix_now_seconds() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

fn new_daemon_interval(period: Duration) -> tokio::time::Interval {
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker
}

fn derive_network_stale_after(heartbeat_ms: u64) -> Duration {
    let heartbeat_ms = heartbeat_ms.max(1_000);
    let stale_ms = heartbeat_ms
        .saturating_mul(4)
        .clamp(MIN_NETWORK_STALE_AFTER_MS, MAX_NETWORK_STALE_AFTER_MS);
    Duration::from_millis(stale_ms)
}

async fn wait_for_shutdown_signal(shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        if shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn run_bootnode_heartbeat(
    endpoints: Vec<BootnodeEndpoint>,
    bootnode_count: usize,
    heartbeat_ms: u64,
    observation: Arc<BootnodeObservation>,
    sync_progress: Arc<Mutex<SyncProgress>>,
    peer_health_tracker: Arc<Mutex<PeerHealthTracker>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut ticker = new_daemon_interval(Duration::from_millis(heartbeat_ms));
    // Consume the immediate first tick so the first probe runs after one full interval.
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = wait_for_shutdown_signal(&mut shutdown_rx) => {
                return;
            }
            _ = ticker.tick() => {
                let reachable = probe_bootnodes(&endpoints, Duration::from_millis(1_500)).await;
                let observed_at = unix_now_seconds();
                observation.update(reachable as u64, observed_at);
                if let Ok(mut progress) = sync_progress.lock() {
                    progress.peer_count = reachable as u64;
                }
                if let Ok(mut tracker) = peer_health_tracker.lock() {
                    tracker.record(reachable as u64, observed_at);
                }
                eprintln!("p2p heartbeat: {reachable}/{bootnode_count} bootnodes reachable");
            }
        }
    }
}

async fn join_task_with_timeout<T>(
    handle: &mut tokio::task::JoinHandle<T>,
    timeout: Duration,
    task_name: &str,
) -> Result<Result<T, tokio::task::JoinError>, String> {
    match tokio::time::timeout(timeout, &mut *handle).await {
        Ok(outcome) => Ok(outcome),
        Err(_) => {
            handle.abort();
            let aborted_outcome = handle.await;
            Err(match aborted_outcome {
                Ok(_) => format!(
                    "{task_name} exceeded graceful shutdown timeout of {:?}; abort requested after completion",
                    timeout
                ),
                Err(join_error) if join_error.is_cancelled() => format!(
                    "{task_name} exceeded graceful shutdown timeout of {:?} and was aborted",
                    timeout
                ),
                Err(join_error) => format!(
                    "{task_name} exceeded graceful shutdown timeout of {:?}; abort join error: {join_error}",
                    timeout
                ),
            })
        }
    }
}

async fn probe_bootnodes(endpoints: &[BootnodeEndpoint], timeout: Duration) -> usize {
    probe_bootnodes_with(endpoints, timeout, &|endpoint, timeout| async move {
        probe_bootnode(&endpoint, timeout).await
    })
    .await
}

async fn probe_bootnodes_with<F, Fut>(
    endpoints: &[BootnodeEndpoint],
    timeout: Duration,
    probe: &F,
) -> usize
where
    F: Fn(BootnodeEndpoint, Duration) -> Fut + Send + Sync,
    Fut: Future<Output = bool> + Send + 'static,
{
    if endpoints.is_empty() {
        return 0;
    }

    let mut reachable = 0usize;
    let max_in_flight = endpoints.len().clamp(1, MAX_BOOTNODE_PROBE_CONCURRENCY);
    let mut next_endpoint = endpoints.iter().cloned();
    let mut probes = tokio::task::JoinSet::new();

    for _ in 0..max_in_flight {
        if let Some(endpoint) = next_endpoint.next() {
            probes.spawn(probe(endpoint, timeout));
        }
    }

    while let Some(result) = probes.join_next().await {
        if let Ok(true) = result {
            reachable = reachable.saturating_add(1);
        }
        if let Some(endpoint) = next_endpoint.next() {
            probes.spawn(probe(endpoint, timeout));
        }
    }
    reachable
}

async fn probe_bootnode(endpoint: &BootnodeEndpoint, timeout: Duration) -> bool {
    match endpoint {
        BootnodeEndpoint::Socket(addr) => {
            matches!(
                tokio::time::timeout(timeout, TcpStream::connect(*addr)).await,
                Ok(Ok(_))
            )
        }
        BootnodeEndpoint::HostPort { host, port } => matches!(
            tokio::time::timeout(timeout, TcpStream::connect((host.as_str(), *port))).await,
            Ok(Ok(_))
        ),
    }
}

async fn healthz(State(state): State<RpcAppState>) -> impl IntoResponse {
    let progress = match state.sync_progress.lock() {
        Ok(progress) => progress.clone(),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "sync progress lock poisoned".to_string(),
            )
                .into_response();
        }
    };

    let peer_health = match state.peer_health_tracker.lock() {
        Ok(tracker) => tracker.snapshot(unix_now_seconds()),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "peer health tracker lock poisoned".to_string(),
            )
                .into_response();
        }
    };

    match evaluate_health(
        &progress,
        &state.health_policy,
        state.min_peer_count,
        &peer_health,
        state.max_recent_peer_health_flaps,
    ) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(message) => (StatusCode::SERVICE_UNAVAILABLE, message).into_response(),
    }
}

async fn readyz(State(state): State<RpcAppState>) -> impl IntoResponse {
    let progress = match state.sync_progress.lock() {
        Ok(progress) => progress.clone(),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "sync progress lock poisoned".to_string(),
            )
                .into_response();
        }
    };

    let peer_health = match state.peer_health_tracker.lock() {
        Ok(tracker) => tracker.snapshot(unix_now_seconds()),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "peer health tracker lock poisoned".to_string(),
            )
                .into_response();
        }
    };

    match evaluate_readiness(
        &progress,
        &state.health_policy,
        state.min_peer_count,
        &peer_health,
        state.max_recent_peer_health_flaps,
    ) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(message) => (StatusCode::SERVICE_UNAVAILABLE, message).into_response(),
    }
}

async fn status(
    State(state): State<RpcAppState>,
    headers: HeaderMap,
) -> Result<Json<StatusPayload>, (StatusCode, String)> {
    if !is_rpc_request_authorized(&headers, state.rpc_auth_token.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "missing or invalid bearer token".to_string(),
        ));
    }

    let progress = state
        .sync_progress
        .lock()
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "sync progress lock poisoned".to_string(),
            )
        })?
        .clone();
    let peer_health = state
        .peer_health_tracker
        .lock()
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "peer health tracker lock poisoned".to_string(),
            )
        })?
        .snapshot(unix_now_seconds());

    Ok(Json(StatusPayload {
        chain_id: state.chain_id,
        starting_block: progress.starting_block,
        current_block: progress.current_block,
        highest_block: progress.highest_block,
        peer_count: progress.peer_count,
        min_peers: state.min_peer_count,
        max_recent_peer_health_flaps: state.max_recent_peer_health_flaps,
        reorg_events: progress.reorg_events,
        consecutive_failures: progress.consecutive_failures,
        last_error: progress.last_error,
        peer_health,
    }))
}

async fn metrics(State(state): State<RpcAppState>, headers: HeaderMap) -> impl IntoResponse {
    if !is_rpc_request_authorized(&headers, state.rpc_auth_token.as_deref()) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "missing or invalid bearer token".to_string(),
        )
            .into_response();
    }

    let metrics = match state.rpc_metrics.lock() {
        Ok(metrics) => metrics,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "rpc metrics lock poisoned".to_string(),
            )
                .into_response();
        }
    };
    let progress = match state.sync_progress.lock() {
        Ok(progress) => progress,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "sync progress lock poisoned".to_string(),
            )
                .into_response();
        }
    };
    let peer_health = match state.peer_health_tracker.lock() {
        Ok(tracker) => tracker.snapshot(unix_now_seconds()),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "peer health tracker lock poisoned".to_string(),
            )
                .into_response();
        }
    };

    let mut body = String::new();
    use std::fmt::Write as _;
    let _ = writeln!(
        body,
        "# HELP pastis_rpc_requests_total Total JSON-RPC requests accepted by the daemon."
    );
    let _ = writeln!(body, "# TYPE pastis_rpc_requests_total counter");
    let _ = writeln!(body, "pastis_rpc_requests_total {}", metrics.requests_total);
    let _ = writeln!(
        body,
        "# HELP pastis_rpc_responses_ok_total Total JSON-RPC responses returned with body."
    );
    let _ = writeln!(body, "# TYPE pastis_rpc_responses_ok_total counter");
    let _ = writeln!(
        body,
        "pastis_rpc_responses_ok_total {}",
        metrics.responses_ok_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_rpc_responses_no_content_total Total JSON-RPC notifications handled without response body."
    );
    let _ = writeln!(body, "# TYPE pastis_rpc_responses_no_content_total counter");
    let _ = writeln!(
        body,
        "pastis_rpc_responses_no_content_total {}",
        metrics.responses_no_content_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_rpc_unauthorized_total Total JSON-RPC requests rejected by auth."
    );
    let _ = writeln!(body, "# TYPE pastis_rpc_unauthorized_total counter");
    let _ = writeln!(
        body,
        "pastis_rpc_unauthorized_total {}",
        metrics.unauthorized_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_rpc_rate_limited_total Total JSON-RPC requests rejected by rate limiting."
    );
    let _ = writeln!(body, "# TYPE pastis_rpc_rate_limited_total counter");
    let _ = writeln!(
        body,
        "pastis_rpc_rate_limited_total {}",
        metrics.rate_limited_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_rpc_concurrency_rejected_total Total JSON-RPC requests rejected by concurrency controls."
    );
    let _ = writeln!(body, "# TYPE pastis_rpc_concurrency_rejected_total counter");
    let _ = writeln!(
        body,
        "pastis_rpc_concurrency_rejected_total {}",
        metrics.concurrency_rejected_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_rpc_internal_error_total Total JSON-RPC requests failed due to daemon internal errors."
    );
    let _ = writeln!(body, "# TYPE pastis_rpc_internal_error_total counter");
    let _ = writeln!(
        body,
        "pastis_rpc_internal_error_total {}",
        metrics.internal_error_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_sync_current_block Current synced block number."
    );
    let _ = writeln!(body, "# TYPE pastis_sync_current_block gauge");
    let _ = writeln!(body, "pastis_sync_current_block {}", progress.current_block);
    let _ = writeln!(
        body,
        "# HELP pastis_sync_highest_block Highest known block number."
    );
    let _ = writeln!(body, "# TYPE pastis_sync_highest_block gauge");
    let _ = writeln!(body, "pastis_sync_highest_block {}", progress.highest_block);
    let _ = writeln!(
        body,
        "# HELP pastis_sync_peer_count Current peer count observed by sync runtime."
    );
    let _ = writeln!(body, "# TYPE pastis_sync_peer_count gauge");
    let _ = writeln!(body, "pastis_sync_peer_count {}", progress.peer_count);
    let _ = writeln!(
        body,
        "# HELP pastis_sync_min_peer_count Minimum healthy peer count configured for the daemon."
    );
    let _ = writeln!(body, "# TYPE pastis_sync_min_peer_count gauge");
    let _ = writeln!(body, "pastis_sync_min_peer_count {}", state.min_peer_count);
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_max_recent_flaps Max allowed recent peer-health flaps before unhealthy (0 disables)."
    );
    let _ = writeln!(body, "# TYPE pastis_peer_health_max_recent_flaps gauge");
    let _ = writeln!(
        body,
        "pastis_peer_health_max_recent_flaps {}",
        state.max_recent_peer_health_flaps
    );
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_status Current peer health state (1=healthy, 0=unhealthy)."
    );
    let _ = writeln!(body, "# TYPE pastis_peer_health_status gauge");
    let _ = writeln!(
        body,
        "pastis_peer_health_status {}",
        if peer_health.healthy { 1 } else { 0 }
    );
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_observations_total Total peer-health observations recorded by the daemon."
    );
    let _ = writeln!(body, "# TYPE pastis_peer_health_observations_total counter");
    let _ = writeln!(
        body,
        "pastis_peer_health_observations_total {}",
        peer_health.observations_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_flap_transitions_total Total health-state flap transitions recorded."
    );
    let _ = writeln!(
        body,
        "# TYPE pastis_peer_health_flap_transitions_total counter"
    );
    let _ = writeln!(
        body,
        "pastis_peer_health_flap_transitions_total {}",
        peer_health.flap_transitions_total
    );
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_flap_transitions_recent Flap transitions within the configured flap window."
    );
    let _ = writeln!(
        body,
        "# TYPE pastis_peer_health_flap_transitions_recent gauge"
    );
    let _ = writeln!(
        body,
        "pastis_peer_health_flap_transitions_recent {}",
        peer_health.flap_transitions_recent
    );
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_history_size Current bounded peer-health history size."
    );
    let _ = writeln!(body, "# TYPE pastis_peer_health_history_size gauge");
    let _ = writeln!(
        body,
        "pastis_peer_health_history_size {}",
        peer_health.history_size
    );
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_last_observed_at_unix_seconds Last peer-health observation timestamp (unix seconds, 0 when unavailable)."
    );
    let _ = writeln!(
        body,
        "# TYPE pastis_peer_health_last_observed_at_unix_seconds gauge"
    );
    let _ = writeln!(
        body,
        "pastis_peer_health_last_observed_at_unix_seconds {}",
        peer_health.last_observed_at_unix_seconds.unwrap_or(0)
    );
    let _ = writeln!(
        body,
        "# HELP pastis_peer_health_last_peer_count Peer count from the last peer-health observation (0 when unavailable)."
    );
    let _ = writeln!(body, "# TYPE pastis_peer_health_last_peer_count gauge");
    let _ = writeln!(
        body,
        "pastis_peer_health_last_peer_count {}",
        peer_health.last_peer_count.unwrap_or(0)
    );
    let _ = writeln!(
        body,
        "# HELP pastis_sync_consecutive_failures Current consecutive sync poll failures."
    );
    let _ = writeln!(body, "# TYPE pastis_sync_consecutive_failures gauge");
    let _ = writeln!(
        body,
        "pastis_sync_consecutive_failures {}",
        progress.consecutive_failures
    );

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

fn parse_daemon_config() -> Result<DaemonConfig, String> {
    let mut cli_upstream_rpc: Option<String> = None;
    let mut cli_rpc_bind: Option<String> = None;
    let mut cli_rpc_auth_token: Option<String> = None;
    let mut cli_allow_public_rpc_bind = false;
    let mut cli_chain_id: Option<ChainId> = None;
    let mut cli_poll_ms: Option<u64> = None;
    let mut cli_replay_window: Option<u64> = None;
    let mut cli_max_replay_per_poll: Option<u64> = None;
    let mut cli_chain_id_revalidate_polls: Option<u64> = None;
    let mut cli_replay_checkpoint_path: Option<String> = None;
    let mut cli_local_journal_path: Option<String> = None;
    let mut cli_storage_snapshot_path: Option<String> = None;
    let mut cli_storage_snapshot_interval_blocks: Option<u64> = None;
    let mut cli_rpc_timeout_secs: Option<u64> = None;
    let mut cli_rpc_max_retries: Option<u32> = None;
    let mut cli_rpc_retry_backoff_ms: Option<u64> = None;
    let mut cli_rpc_max_concurrency: Option<usize> = None;
    let mut cli_rpc_rate_limit_per_minute: Option<u32> = None;
    let mut cli_disable_batch_requests = false;
    let mut cli_strict_canonical_execution: Option<bool> = None;
    let mut cli_bootnodes: Vec<String> = Vec::new();
    let mut cli_require_peers = false;
    let mut cli_exit_on_unhealthy = false;
    let mut cli_p2p_heartbeat_ms: Option<u64> = None;
    let mut cli_min_peers: Option<u64> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--upstream-rpc-url" => {
                cli_upstream_rpc = Some(
                    args.next()
                        .ok_or_else(|| "--upstream-rpc-url requires a value".to_string())?,
                );
            }
            "--rpc-bind" => {
                cli_rpc_bind = Some(
                    args.next()
                        .ok_or_else(|| "--rpc-bind requires a value".to_string())?,
                );
            }
            "--rpc-auth-token" => {
                cli_rpc_auth_token = Some(
                    args.next()
                        .ok_or_else(|| "--rpc-auth-token requires a value".to_string())?,
                );
            }
            "--allow-public-rpc-bind" => {
                cli_allow_public_rpc_bind = true;
            }
            "--chain-id" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--chain-id requires a value".to_string())?;
                let parsed = ChainId::parse(raw)
                    .map_err(|error| format!("invalid --chain-id value: {error}"))?;
                cli_chain_id = Some(parsed);
            }
            "--poll-ms" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--poll-ms requires a value".to_string())?;
                cli_poll_ms = Some(parse_positive_u64(&raw, "--poll-ms")?);
            }
            "--replay-window" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--replay-window requires a value".to_string())?;
                cli_replay_window = Some(parse_positive_u64(&raw, "--replay-window")?);
            }
            "--max-replay-per-poll" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--max-replay-per-poll requires a value".to_string())?;
                cli_max_replay_per_poll = Some(parse_positive_u64(&raw, "--max-replay-per-poll")?);
            }
            "--chain-id-revalidate-polls" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--chain-id-revalidate-polls requires a value".to_string())?;
                cli_chain_id_revalidate_polls =
                    Some(parse_positive_u64(&raw, "--chain-id-revalidate-polls")?);
            }
            "--replay-checkpoint" => {
                cli_replay_checkpoint_path = Some(
                    args.next()
                        .ok_or_else(|| "--replay-checkpoint requires a value".to_string())?,
                );
            }
            "--local-journal" => {
                cli_local_journal_path = Some(
                    args.next()
                        .ok_or_else(|| "--local-journal requires a value".to_string())?,
                );
            }
            "--storage-snapshot" => {
                cli_storage_snapshot_path = Some(
                    args.next()
                        .ok_or_else(|| "--storage-snapshot requires a value".to_string())?,
                );
            }
            "--storage-snapshot-interval-blocks" => {
                let raw = args.next().ok_or_else(|| {
                    "--storage-snapshot-interval-blocks requires a value".to_string()
                })?;
                cli_storage_snapshot_interval_blocks = Some(parse_positive_u64(
                    &raw,
                    "--storage-snapshot-interval-blocks",
                )?);
            }
            "--rpc-timeout-secs" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--rpc-timeout-secs requires a value".to_string())?;
                cli_rpc_timeout_secs = Some(parse_positive_u64(&raw, "--rpc-timeout-secs")?);
            }
            "--rpc-max-retries" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--rpc-max-retries requires a value".to_string())?;
                cli_rpc_max_retries = Some(parse_non_negative_u32(&raw, "--rpc-max-retries")?);
            }
            "--rpc-retry-backoff-ms" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--rpc-retry-backoff-ms requires a value".to_string())?;
                cli_rpc_retry_backoff_ms =
                    Some(parse_positive_u64(&raw, "--rpc-retry-backoff-ms")?);
            }
            "--rpc-max-concurrency" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--rpc-max-concurrency requires a value".to_string())?;
                cli_rpc_max_concurrency =
                    Some(parse_positive_usize(&raw, "--rpc-max-concurrency")?);
            }
            "--rpc-rate-limit-per-minute" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--rpc-rate-limit-per-minute requires a value".to_string())?;
                cli_rpc_rate_limit_per_minute =
                    Some(parse_non_negative_u32(&raw, "--rpc-rate-limit-per-minute")?);
            }
            "--disable-upstream-batch" => {
                cli_disable_batch_requests = true;
            }
            "--strict-canonical-execution" => {
                cli_strict_canonical_execution = Some(true);
            }
            "--no-strict-canonical-execution" => {
                cli_strict_canonical_execution = Some(false);
            }
            "--bootnode" => {
                cli_bootnodes.push(
                    args.next()
                        .ok_or_else(|| "--bootnode requires a value".to_string())?,
                );
            }
            "--require-peers" => {
                cli_require_peers = true;
            }
            "--exit-on-unhealthy" => {
                cli_exit_on_unhealthy = true;
            }
            "--p2p-heartbeat-ms" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--p2p-heartbeat-ms requires a value".to_string())?;
                cli_p2p_heartbeat_ms = Some(parse_positive_u64(&raw, "--p2p-heartbeat-ms")?);
            }
            "--min-peers" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--min-peers requires a value".to_string())?;
                cli_min_peers = Some(parse_non_negative_u64(&raw, "--min-peers")?);
            }
            "--help" | "-h" => {
                return Err(help_text());
            }
            unknown => {
                if let Some(raw_value) = unknown.strip_prefix("--strict-canonical-execution=") {
                    cli_strict_canonical_execution = Some(parse_bool_literal(
                        raw_value,
                        "--strict-canonical-execution",
                    )?);
                    continue;
                }
                return Err(format!("unknown flag `{unknown}`\n\n{}", help_text()));
            }
        }
    }

    let upstream_rpc_url = cli_upstream_rpc
        .or_else(|| env::var("STARKNET_RPC_URL").ok())
        .ok_or_else(|| {
            "missing upstream RPC URL; pass --upstream-rpc-url or set STARKNET_RPC_URL".to_string()
        })?;
    let _validated_upstreams = parse_upstream_rpc_urls(&upstream_rpc_url)?;

    let chain_id = match cli_chain_id {
        Some(id) => id,
        None => {
            let raw = env::var("PASTIS_CHAIN_ID").unwrap_or_else(|_| "SN_MAIN".to_string());
            ChainId::parse(raw).map_err(|error| format!("invalid PASTIS_CHAIN_ID: {error}"))?
        }
    };

    let allow_public_rpc_bind = if cli_allow_public_rpc_bind {
        true
    } else {
        parse_env_bool("PASTIS_ALLOW_PUBLIC_RPC_BIND")?.unwrap_or(false)
    };

    let mut bootnodes = cli_bootnodes;
    if let Ok(raw_bootnodes) = env::var("PASTIS_BOOTNODES") {
        for entry in raw_bootnodes.split(',') {
            let trimmed = entry.trim();
            if !trimmed.is_empty() {
                bootnodes.push(trimmed.to_string());
            }
        }
    }
    validate_bootnode_inputs(&bootnodes)?;

    let poll_ms = match cli_poll_ms {
        Some(value) => value,
        None => parse_env_u64("PASTIS_NODE_POLL_MS")?.unwrap_or(DEFAULT_SYNC_POLL_MS),
    };
    let replay_window = match cli_replay_window {
        Some(value) => value,
        None => parse_env_u64("PASTIS_REPLAY_WINDOW")?.unwrap_or(DEFAULT_REPLAY_WINDOW),
    };
    let max_replay_per_poll = match cli_max_replay_per_poll {
        Some(value) => value,
        None => parse_env_u64("PASTIS_MAX_REPLAY_PER_POLL")?.unwrap_or(DEFAULT_MAX_REPLAY_PER_POLL),
    };
    let chain_id_revalidate_polls = match cli_chain_id_revalidate_polls {
        Some(value) => value,
        None => parse_env_u64("PASTIS_CHAIN_ID_REVALIDATE_POLLS")?
            .unwrap_or(DEFAULT_CHAIN_ID_REVALIDATE_POLLS),
    };
    let rpc_timeout_secs = match cli_rpc_timeout_secs {
        Some(value) => value,
        None => parse_env_u64("PASTIS_RPC_TIMEOUT_SECS")?.unwrap_or(DEFAULT_RPC_TIMEOUT_SECS),
    };
    let rpc_max_retries = match cli_rpc_max_retries {
        Some(value) => value,
        None => parse_env_u32("PASTIS_RPC_MAX_RETRIES")?.unwrap_or(DEFAULT_RPC_MAX_RETRIES),
    };
    let rpc_retry_backoff_ms = match cli_rpc_retry_backoff_ms {
        Some(value) => value,
        None => {
            parse_env_u64("PASTIS_RPC_RETRY_BACKOFF_MS")?.unwrap_or(DEFAULT_RPC_RETRY_BACKOFF_MS)
        }
    };
    let rpc_max_concurrency = match cli_rpc_max_concurrency {
        Some(value) => value,
        None => {
            parse_env_usize("PASTIS_RPC_MAX_CONCURRENCY")?.unwrap_or(DEFAULT_RPC_MAX_CONCURRENCY)
        }
    };
    let rpc_rate_limit_per_minute = match cli_rpc_rate_limit_per_minute {
        Some(value) => value,
        None => parse_env_u32("PASTIS_RPC_RATE_LIMIT_PER_MINUTE")?
            .unwrap_or(DEFAULT_RPC_RATE_LIMIT_PER_MINUTE),
    };
    let rpc_rate_limit_per_minute = validate_rpc_rate_limit_per_minute(rpc_rate_limit_per_minute)?;
    let disable_batch_requests = if cli_disable_batch_requests {
        true
    } else {
        parse_env_bool("PASTIS_DISABLE_UPSTREAM_BATCH")?.unwrap_or(false)
    };
    let strict_canonical_execution = resolve_strict_canonical_execution(
        cli_strict_canonical_execution,
        parse_env_bool("PASTIS_STRICT_CANONICAL_EXECUTION")?,
        default_strict_canonical_execution(),
    );
    let p2p_heartbeat_ms = match cli_p2p_heartbeat_ms {
        Some(value) => value,
        None => parse_env_u64("PASTIS_P2P_HEARTBEAT_MS")?.unwrap_or(DEFAULT_P2P_HEARTBEAT_MS),
    };
    let peer_health_history_capacity = parse_env_usize("PASTIS_PEER_HEALTH_HISTORY_CAPACITY")?
        .unwrap_or(DEFAULT_PEER_HEALTH_HISTORY_CAPACITY);
    let peer_health_flap_window_secs = parse_env_u64("PASTIS_PEER_HEALTH_FLAP_WINDOW_SECS")?
        .unwrap_or(DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS);
    let peer_health_max_recent_flaps =
        parse_env_non_negative_u64("PASTIS_PEER_HEALTH_MAX_RECENT_FLAPS")?
            .unwrap_or(DEFAULT_PEER_HEALTH_MAX_RECENT_FLAPS);
    let storage_snapshot_interval_blocks = match cli_storage_snapshot_interval_blocks {
        Some(value) => value,
        None => parse_env_u64("PASTIS_STORAGE_SNAPSHOT_INTERVAL_BLOCKS")?
            .unwrap_or(DEFAULT_STORAGE_SNAPSHOT_INTERVAL_BLOCKS),
    };
    let health_max_consecutive_failures = parse_env_u64("PASTIS_HEALTH_MAX_CONSECUTIVE_FAILURES")?
        .unwrap_or(DEFAULT_HEALTH_MAX_CONSECUTIVE_FAILURES);
    let health_max_sync_lag_blocks = parse_env_u64("PASTIS_HEALTH_MAX_SYNC_LAG_BLOCKS")?
        .unwrap_or(DEFAULT_HEALTH_MAX_SYNC_LAG_BLOCKS);
    let env_require_peers = parse_env_bool("PASTIS_REQUIRE_PEERS")?.unwrap_or(false);
    let env_min_peers = parse_env_non_negative_u64("PASTIS_MIN_PEERS")?;
    let min_peers = if let Some(value) = cli_min_peers {
        value
    } else if cli_require_peers {
        1
    } else if let Some(value) = env_min_peers {
        value
    } else if env_require_peers {
        1
    } else {
        0
    };
    let exit_on_unhealthy = if cli_exit_on_unhealthy {
        true
    } else {
        parse_env_bool("PASTIS_EXIT_ON_UNHEALTHY")?.unwrap_or(false)
    };

    let rpc_auth_token = match cli_rpc_auth_token.or_else(|| env::var("PASTIS_RPC_AUTH_TOKEN").ok())
    {
        Some(raw) => Some(validate_rpc_auth_token(raw)?),
        None => None,
    };

    let rpc_bind = cli_rpc_bind
        .or_else(|| env::var("PASTIS_NODE_RPC_BIND").ok())
        .unwrap_or_else(|| DEFAULT_RPC_BIND.to_string());
    validate_rpc_bind_exposure(&rpc_bind, rpc_auth_token.as_deref(), allow_public_rpc_bind)?;

    Ok(DaemonConfig {
        upstream_rpc_url,
        rpc_bind,
        rpc_auth_token,
        allow_public_rpc_bind,
        chain_id,
        poll_ms,
        replay_window,
        max_replay_per_poll,
        chain_id_revalidate_polls,
        replay_checkpoint_path: cli_replay_checkpoint_path
            .or_else(|| env::var("PASTIS_REPLAY_CHECKPOINT_PATH").ok())
            .or_else(|| Some(DEFAULT_REPLAY_CHECKPOINT_PATH.to_string())),
        local_journal_path: cli_local_journal_path
            .or_else(|| env::var("PASTIS_LOCAL_JOURNAL_PATH").ok())
            .or_else(|| Some(DEFAULT_LOCAL_JOURNAL_PATH.to_string())),
        storage_snapshot_path: cli_storage_snapshot_path
            .or_else(|| env::var("PASTIS_STORAGE_SNAPSHOT_PATH").ok())
            .or_else(|| Some(DEFAULT_STORAGE_SNAPSHOT_PATH.to_string())),
        storage_snapshot_interval_blocks,
        rpc_timeout_secs,
        rpc_max_retries,
        rpc_retry_backoff_ms,
        rpc_max_concurrency,
        rpc_rate_limit_per_minute,
        disable_batch_requests,
        strict_canonical_execution,
        bootnodes,
        min_peers,
        p2p_heartbeat_ms,
        peer_health_history_capacity,
        peer_health_flap_window_secs,
        peer_health_max_recent_flaps,
        health_max_consecutive_failures,
        health_max_sync_lag_blocks,
        exit_on_unhealthy,
    })
}

fn parse_env_u64(name: &str) -> Result<Option<u64>, String> {
    match env::var(name) {
        Ok(raw) => Ok(Some(parse_positive_u64(&raw, name)?)),
        Err(_) => Ok(None),
    }
}

fn parse_env_non_negative_u64(name: &str) -> Result<Option<u64>, String> {
    match env::var(name) {
        Ok(raw) => Ok(Some(parse_non_negative_u64(&raw, name)?)),
        Err(_) => Ok(None),
    }
}

fn parse_env_u32(name: &str) -> Result<Option<u32>, String> {
    match env::var(name) {
        Ok(raw) => Ok(Some(parse_non_negative_u32(&raw, name)?)),
        Err(_) => Ok(None),
    }
}

fn parse_env_usize(name: &str) -> Result<Option<usize>, String> {
    match env::var(name) {
        Ok(raw) => Ok(Some(parse_positive_usize(&raw, name)?)),
        Err(_) => Ok(None),
    }
}

fn parse_env_bool(name: &str) -> Result<Option<bool>, String> {
    match env::var(name) {
        Ok(raw) => Ok(Some(parse_bool_literal(&raw, name)?)),
        Err(_) => Ok(None),
    }
}

fn parse_bool_literal(raw: &str, field: &str) -> Result<bool, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "invalid {field} value `{raw}`: expected one of true/false/1/0/yes/no/on/off"
        )),
    }
}

fn resolve_strict_canonical_execution(
    cli_override: Option<bool>,
    env_override: Option<bool>,
    default: bool,
) -> bool {
    cli_override.or(env_override).unwrap_or(default)
}

fn default_strict_canonical_execution() -> bool {
    cfg!(feature = "production-adapters")
}

fn parse_positive_u64(raw: &str, field: &str) -> Result<u64, String> {
    let parsed = raw
        .parse::<u64>()
        .map_err(|error| format!("invalid {field} value `{raw}`: {error}"))?;
    if parsed == 0 {
        return Err(format!("invalid {field} value `{raw}`: must be > 0"));
    }
    Ok(parsed)
}

fn parse_non_negative_u32(raw: &str, field: &str) -> Result<u32, String> {
    raw.parse::<u32>()
        .map_err(|error| format!("invalid {field} value `{raw}`: {error}"))
}

fn parse_non_negative_u64(raw: &str, field: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|error| format!("invalid {field} value `{raw}`: {error}"))
}

fn parse_positive_usize(raw: &str, field: &str) -> Result<usize, String> {
    let parsed = raw
        .parse::<usize>()
        .map_err(|error| format!("invalid {field} value `{raw}`: {error}"))?;
    if parsed == 0 {
        return Err(format!("invalid {field} value `{raw}`: must be > 0"));
    }
    Ok(parsed)
}

fn parse_upstream_rpc_urls(raw: &str) -> Result<Vec<String>, String> {
    if raw.trim().is_empty() {
        return Err("invalid upstream RPC URL list: empty input".to_string());
    }

    let mut urls = Vec::new();
    let mut seen = HashSet::new();
    for entry in raw.split(',') {
        let candidate = entry.trim();
        if candidate.is_empty() {
            continue;
        }
        if candidate.len() > MAX_UPSTREAM_RPC_URL_BYTES {
            return Err(format!(
                "invalid upstream RPC URL entry: exceeds max {} bytes",
                MAX_UPSTREAM_RPC_URL_BYTES
            ));
        }
        if candidate.chars().any(char::is_control) {
            return Err(
                "invalid upstream RPC URL entry: must not contain control characters".to_string(),
            );
        }
        let parsed = reqwest::Url::parse(candidate)
            .map_err(|error| format!("invalid upstream RPC URL entry: {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err("invalid upstream RPC URL entry: scheme must be http or https".to_string());
        }
        if parsed.host_str().is_none() {
            return Err("invalid upstream RPC URL entry: host is required".to_string());
        }
        if seen.insert(candidate.to_string()) {
            if urls.len().saturating_add(1) > MAX_UPSTREAM_RPC_ENDPOINTS {
                return Err(format!(
                    "invalid upstream RPC URL list: {} endpoints configured; max supported is {}",
                    urls.len().saturating_add(1),
                    MAX_UPSTREAM_RPC_ENDPOINTS
                ));
            }
            urls.push(candidate.to_string());
        }
    }

    if urls.is_empty() {
        return Err("invalid upstream RPC URL list: no valid URLs provided".to_string());
    }
    Ok(urls)
}

fn validate_rpc_rate_limit_per_minute(value: u32) -> Result<u32, String> {
    if value > MAX_RPC_RATE_LIMIT_PER_MINUTE {
        return Err(format!(
            "invalid rpc rate limit value `{value}`: exceeds max {MAX_RPC_RATE_LIMIT_PER_MINUTE} requests/minute"
        ));
    }
    Ok(value)
}

fn parse_bootnode_endpoints(bootnodes: &[String]) -> Result<Vec<BootnodeEndpoint>, String> {
    let mut seen = HashSet::with_capacity(bootnodes.len());
    let mut parsed = Vec::with_capacity(bootnodes.len());
    for raw in bootnodes {
        let Some(endpoint) = parse_bootnode_endpoint(raw) else {
            return Err(format!(
                "invalid bootnode `{raw}`: expected socket address, host:port, or /ip4|ip6|dns*/.../tcp/<port> multiaddr"
            ));
        };
        if seen.insert(endpoint.clone()) {
            parsed.push(endpoint);
        }
    }
    Ok(parsed)
}

fn parse_bootnode_endpoint(raw: &str) -> Option<BootnodeEndpoint> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(addr) = trimmed.parse::<SocketAddr>() {
        if addr.port() == 0 {
            return None;
        }
        return Some(BootnodeEndpoint::Socket(addr));
    }
    if trimmed.starts_with('/') {
        let segments: Vec<&str> = trimmed
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        if segments.len() >= 4 && segments[2] == "tcp" {
            let port = segments[3].parse::<u16>().ok()?;
            if port == 0 {
                return None;
            }
            match segments[0] {
                "ip4" | "ip6" => {
                    let host = segments[1];
                    if let Ok(addr) = format!("{host}:{port}").parse::<SocketAddr>() {
                        return Some(BootnodeEndpoint::Socket(addr));
                    }
                    if let Ok(addr) = format!("[{host}]:{port}").parse::<SocketAddr>() {
                        return Some(BootnodeEndpoint::Socket(addr));
                    }
                }
                "dns" | "dns4" | "dns6" => {
                    return Some(BootnodeEndpoint::HostPort {
                        host: segments[1].to_string(),
                        port,
                    });
                }
                _ => {}
            }
        }
    }
    if let Some((host, port)) = trimmed.rsplit_once(':') {
        let parsed_port = port.parse::<u16>().ok()?;
        if parsed_port == 0 {
            return None;
        }
        let normalized_host = host.trim().trim_start_matches('[').trim_end_matches(']');
        if normalized_host.is_empty() {
            return None;
        }
        if normalized_host.chars().any(char::is_whitespace) {
            return None;
        }
        if let Ok(ip) = normalized_host.parse::<IpAddr>() {
            return Some(BootnodeEndpoint::Socket(SocketAddr::new(ip, parsed_port)));
        }
        return Some(BootnodeEndpoint::HostPort {
            host: normalized_host.to_string(),
            port: parsed_port,
        });
    }
    None
}

fn validate_rpc_auth_token(raw: String) -> Result<String, String> {
    if raw.is_empty() {
        return Err("invalid RPC auth token: token must not be empty".to_string());
    }
    if raw.len() > MAX_RPC_AUTH_TOKEN_BYTES {
        return Err(format!(
            "invalid RPC auth token: exceeds max {} bytes",
            MAX_RPC_AUTH_TOKEN_BYTES
        ));
    }
    if raw.chars().any(char::is_whitespace) {
        return Err(
            "invalid RPC auth token: token must not contain whitespace characters".to_string(),
        );
    }
    Ok(raw)
}

fn validate_rpc_bind_exposure(
    rpc_bind: &str,
    rpc_auth_token: Option<&str>,
    allow_public_rpc_bind: bool,
) -> Result<(), String> {
    if allow_public_rpc_bind {
        return Ok(());
    }
    let Ok(parsed) = rpc_bind.parse::<SocketAddr>() else {
        let Some((host, _port)) = rpc_bind.rsplit_once(':') else {
            return Ok(());
        };
        let normalized_host = host.trim().trim_start_matches('[').trim_end_matches(']');
        if normalized_host.eq_ignore_ascii_case("localhost")
            || normalized_host == "127.0.0.1"
            || normalized_host == "::1"
        {
            return Ok(());
        }
        if rpc_auth_token.is_some() {
            return Ok(());
        }
        return Err(format!(
            "refusing non-loopback rpc bind `{rpc_bind}` without auth token; set --rpc-auth-token/PASTIS_RPC_AUTH_TOKEN or --allow-public-rpc-bind/PASTIS_ALLOW_PUBLIC_RPC_BIND=true"
        ));
    };
    if parsed.ip().is_loopback() {
        return Ok(());
    }
    if rpc_auth_token.is_some() {
        return Ok(());
    }
    Err(format!(
        "refusing non-loopback rpc bind `{rpc_bind}` without auth token; set --rpc-auth-token/PASTIS_RPC_AUTH_TOKEN or --allow-public-rpc-bind/PASTIS_ALLOW_PUBLIC_RPC_BIND=true"
    ))
}

fn validate_bootnode_inputs(bootnodes: &[String]) -> Result<(), String> {
    if bootnodes.len() > MAX_BOOTNODES {
        return Err(format!(
            "too many bootnodes configured: {} (max {MAX_BOOTNODES})",
            bootnodes.len()
        ));
    }

    for (index, bootnode) in bootnodes.iter().enumerate() {
        let trimmed = bootnode.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.len() > MAX_BOOTNODE_ENTRY_BYTES {
            return Err(format!(
                "bootnode entry at index {index} exceeds max {MAX_BOOTNODE_ENTRY_BYTES} bytes"
            ));
        }
        if trimmed.chars().any(char::is_control) {
            return Err(format!(
                "bootnode entry at index {index} contains control characters"
            ));
        }
    }

    Ok(())
}

fn validate_peer_requirements(min_peers: u64, bootnode_count: usize) -> Result<(), String> {
    if min_peers == 0 {
        return Ok(());
    }
    if bootnode_count == 0 {
        return Err(
            "min peer requirement is enabled but no valid bootnodes were configured; set --bootnode/PASTIS_BOOTNODES or lower --min-peers/PASTIS_MIN_PEERS"
                .to_string(),
        );
    }
    if min_peers > bootnode_count as u64 {
        return Err(format!(
            "min peer requirement {min_peers} exceeds configured bootnode count {bootnode_count}"
        ));
    }
    Ok(())
}

fn redact_rpc_url(raw: &str) -> String {
    match reqwest::Url::parse(raw) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("unknown-host");
            let port = url
                .port()
                .map(|value| format!(":{value}"))
                .unwrap_or_default();
            format!("{}://{}{port}", url.scheme(), host)
        }
        Err(_) => "<invalid-rpc-url>".to_string(),
    }
}

fn redact_rpc_urls(raw: &str) -> String {
    parse_upstream_rpc_urls(raw)
        .map(|urls| {
            urls.iter()
                .map(|url| redact_rpc_url(url))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_else(|_| redact_rpc_url(raw))
}

fn evaluate_health(
    progress: &SyncProgress,
    policy: &HealthPolicy,
    min_peer_count: u64,
    peer_health: &PeerHealthStatus,
    max_recent_peer_health_flaps: u64,
) -> Result<(), String> {
    let required_peers = if min_peer_count > 0 {
        min_peer_count
    } else if policy.require_peers {
        1
    } else {
        0
    };
    if progress.peer_count < required_peers {
        return Err(format!(
            "unhealthy: peer_count={} below required minimum={required_peers}",
            progress.peer_count
        ));
    }

    if progress.consecutive_failures > policy.max_consecutive_failures {
        return Err(format!(
            "unhealthy: consecutive_failures={} exceeds threshold={}; last_error={}",
            progress.consecutive_failures,
            policy.max_consecutive_failures,
            progress
                .last_error
                .clone()
                .unwrap_or_else(|| "none".to_string())
        ));
    }

    let sync_lag = progress
        .highest_block
        .saturating_sub(progress.current_block);
    if sync_lag > policy.max_sync_lag {
        return Err(format!(
            "unhealthy: sync_lag={} exceeds threshold={} (current={}, highest={})",
            sync_lag, policy.max_sync_lag, progress.current_block, progress.highest_block
        ));
    }

    if max_recent_peer_health_flaps > 0
        && peer_health.flap_transitions_recent > max_recent_peer_health_flaps
    {
        return Err(format!(
            "unhealthy: peer_health_flap_transitions_recent={} exceeds threshold={}",
            peer_health.flap_transitions_recent, max_recent_peer_health_flaps
        ));
    }

    Ok(())
}

fn evaluate_readiness(
    progress: &SyncProgress,
    policy: &HealthPolicy,
    min_peer_count: u64,
    peer_health: &PeerHealthStatus,
    max_recent_peer_health_flaps: u64,
) -> Result<(), String> {
    evaluate_health(
        progress,
        policy,
        min_peer_count,
        peer_health,
        max_recent_peer_health_flaps,
    )?;
    if progress.current_block < progress.highest_block {
        return Err(format!(
            "not ready: sync in progress (current={}, highest={})",
            progress.current_block, progress.highest_block
        ));
    }
    Ok(())
}

fn classify_rpc_task_completion(
    outcome: Result<Result<(), String>, tokio::task::JoinError>,
    allow_clean_exit: bool,
) -> Result<(), String> {
    match outcome {
        Ok(Ok(())) => {
            if allow_clean_exit {
                Ok(())
            } else {
                Err("rpc server exited unexpectedly".to_string())
            }
        }
        Ok(Err(error)) => Err(error),
        Err(error) => Err(format!("rpc server task join error: {error}")),
    }
}

fn unhealthy_exit_reason(
    progress: &SyncProgress,
    policy: &HealthPolicy,
    min_peer_count: u64,
    exit_on_unhealthy: bool,
    peer_health: PeerHealthStatus,
    max_recent_peer_health_flaps: u64,
) -> Option<String> {
    if !exit_on_unhealthy {
        return None;
    }
    evaluate_health(
        progress,
        policy,
        min_peer_count,
        &peer_health,
        max_recent_peer_health_flaps,
    )
    .err()
}

fn help_text() -> String {
    format!(
        "usage: starknet-node --upstream-rpc-url <url[,url...]> [options]\n\
options:\n\
  --rpc-bind <addr>                  JSON-RPC bind address (default: {DEFAULT_RPC_BIND})\n\
  --rpc-auth-token <token>           Require bearer token for POST / JSON-RPC\n\
  --allow-public-rpc-bind            Allow non-loopback bind without auth token (unsafe)\n\
  --chain-id <id>                    Chain id (default: SN_MAIN)\n\
  --poll-ms <ms>                     Sync poll interval in milliseconds\n\
  --replay-window <blocks>           Replay window size\n\
  --max-replay-per-poll <blocks>     Max blocks to process per poll\n\
  --chain-id-revalidate-polls <n>    Polls between upstream chain-id checks (default: {DEFAULT_CHAIN_ID_REVALIDATE_POLLS})\n\
  --replay-checkpoint <path>         Replay checkpoint file path\n\
  --local-journal <path>             Local block/state journal path\n\
  --storage-snapshot <path>          Runtime storage snapshot path\n\
  --storage-snapshot-interval-blocks <n>\n\
                                     Snapshot every N committed local blocks (default: {DEFAULT_STORAGE_SNAPSHOT_INTERVAL_BLOCKS})\n\
  --rpc-timeout-secs <secs>          Upstream RPC timeout\n\
  --rpc-max-retries <n>              Upstream RPC max retries\n\
  --rpc-retry-backoff-ms <ms>        Retry backoff base\n\
  --rpc-max-concurrency <n>          Max concurrent local RPC requests (default: {DEFAULT_RPC_MAX_CONCURRENCY})\n\
  --rpc-rate-limit-per-minute <n>    Per-IP RPC request rate limit (0 disables; default: {DEFAULT_RPC_RATE_LIMIT_PER_MINUTE})\n\
  --disable-upstream-batch           Disable outbound upstream JSON-RPC batch requests\n\
  --strict-canonical-execution[=bool]\n\
                                     Require canonical execution for committed blocks (CLI overrides env; default=true with production-adapters)\n\
  --no-strict-canonical-execution    Disable strict canonical execution (equivalent to --strict-canonical-execution=false)\n\
  --bootnode <multiaddr>             Configure bootnode (repeatable)\n\
  --require-peers                    Fail closed when no peers are configured/available\n\
  --min-peers <n>                    Minimum healthy peer count requirement (0 disables; implies require-peers when n > 0)\n\
  --exit-on-unhealthy                Exit daemon when health checks fail\n\
  --p2p-heartbeat-ms <ms>            P2P heartbeat logging interval\n\
environment:\n\
  STARKNET_RPC_URL                   Upstream Starknet RPC URL (single URL or comma-separated failover list)\n\
  PASTIS_NODE_RPC_BIND               Local JSON-RPC bind\n\
  PASTIS_RPC_AUTH_TOKEN              Bearer token required for POST / JSON-RPC\n\
  PASTIS_ALLOW_PUBLIC_RPC_BIND       Allow non-loopback RPC bind without token (true/false)\n\
  PASTIS_CHAIN_ID                    Node chain id\n\
  PASTIS_NODE_POLL_MS                Sync poll interval\n\
  PASTIS_REPLAY_WINDOW               Replay window\n\
  PASTIS_MAX_REPLAY_PER_POLL         Max replay per poll\n\
  PASTIS_CHAIN_ID_REVALIDATE_POLLS   Polls between upstream chain-id checks\n\
  PASTIS_REPLAY_CHECKPOINT_PATH      Replay checkpoint path\n\
  PASTIS_LOCAL_JOURNAL_PATH          Local block/state journal path\n\
  PASTIS_STORAGE_SNAPSHOT_PATH       Runtime storage snapshot path\n\
  PASTIS_STORAGE_SNAPSHOT_INTERVAL_BLOCKS   Snapshot every N committed local blocks\n\
  PASTIS_RPC_TIMEOUT_SECS            Upstream RPC timeout seconds\n\
  PASTIS_RPC_MAX_RETRIES             Upstream RPC max retries\n\
  PASTIS_RPC_RETRY_BACKOFF_MS        Upstream RPC retry backoff ms\n\
  PASTIS_RPC_MAX_CONCURRENCY         Max concurrent local RPC requests\n\
  PASTIS_RPC_RATE_LIMIT_PER_MINUTE   Per-IP RPC request rate limit (0 disables)\n\
  PASTIS_DISABLE_UPSTREAM_BATCH      Disable outbound upstream JSON-RPC batch requests (true/false)\n\
  PASTIS_STRICT_CANONICAL_EXECUTION  Require canonical execution for committed blocks (true/false; used only when strict canonical CLI flags are absent; default=true with production-adapters)\n\
  PASTIS_BOOTNODES                   Comma-separated bootnodes\n\
  PASTIS_REQUIRE_PEERS               Require peers for sync loop health (true/false)\n\
  PASTIS_MIN_PEERS                   Minimum healthy peer count (0 disables)\n\
  PASTIS_EXIT_ON_UNHEALTHY           Exit daemon when health checks fail (true/false)\n\
  PASTIS_P2P_HEARTBEAT_MS            P2P heartbeat interval\n\
  PASTIS_PEER_HEALTH_HISTORY_CAPACITY  Bounded peer-health history capacity (default: {DEFAULT_PEER_HEALTH_HISTORY_CAPACITY})\n\
  PASTIS_PEER_HEALTH_FLAP_WINDOW_SECS  Flap detection lookback window in seconds (default: {DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS})\n\
  PASTIS_PEER_HEALTH_MAX_RECENT_FLAPS  Max allowed recent peer-health flaps before unhealthy (0 disables; default: {DEFAULT_PEER_HEALTH_MAX_RECENT_FLAPS})\n\
  PASTIS_HEALTH_MAX_CONSECUTIVE_FAILURES   Health failure threshold for consecutive poll failures\n\
  PASTIS_HEALTH_MAX_SYNC_LAG_BLOCKS         Health failure threshold for sync lag in blocks"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Instant;

    use axum::body::to_bytes;
    use axum::http::HeaderValue;
    use serde_json::Value;
    use tokio::sync::Semaphore;

    use starknet_node_storage::{InMemoryStorage, ThreadSafeStorage};
    use starknet_node_types::InMemoryState;

    use super::*;

    fn peer(port: u16) -> ConnectInfo<SocketAddr> {
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], port)))
    }

    fn base_progress() -> SyncProgress {
        SyncProgress {
            starting_block: 10,
            current_block: 10,
            highest_block: 10,
            peer_count: 3,
            reorg_events: 0,
            consecutive_failures: 0,
            last_error: None,
        }
    }

    fn base_peer_health_status() -> PeerHealthStatus {
        PeerHealthStatus {
            healthy: true,
            last_peer_count: Some(3),
            history_size: 1,
            observations_total: 1,
            healthy_observations_total: 1,
            unhealthy_observations_total: 0,
            flap_transitions_total: 0,
            flap_transitions_recent: 0,
            last_observed_at_unix_seconds: Some(10),
        }
    }

    #[test]
    fn evaluate_health_reports_healthy_when_within_thresholds() {
        let progress = base_progress();
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        assert!(evaluate_health(&progress, &policy, 0, &peer_health, 0).is_ok());
    }

    #[tokio::test]
    async fn daemon_interval_uses_delay_missed_tick_behavior() {
        let ticker = new_daemon_interval(Duration::from_millis(100));
        assert_eq!(
            ticker.missed_tick_behavior(),
            tokio::time::MissedTickBehavior::Delay
        );
    }

    #[test]
    fn derive_network_stale_after_scales_with_heartbeat_and_respects_bounds() {
        assert_eq!(
            derive_network_stale_after(250),
            Duration::from_millis(MIN_NETWORK_STALE_AFTER_MS)
        );
        assert_eq!(
            derive_network_stale_after(30_000),
            Duration::from_millis(120_000)
        );
        assert_eq!(
            derive_network_stale_after(500_000),
            Duration::from_millis(MAX_NETWORK_STALE_AFTER_MS)
        );
    }

    #[test]
    fn evaluate_readiness_is_ready_when_synced_and_healthy() {
        let progress = base_progress();
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        assert!(evaluate_readiness(&progress, &policy, 0, &peer_health, 0).is_ok());
    }

    #[test]
    fn evaluate_readiness_fails_when_node_is_still_catching_up() {
        let mut progress = base_progress();
        progress.current_block = 10;
        progress.highest_block = 20;
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        let error = evaluate_readiness(&progress, &policy, 0, &peer_health, 0)
            .expect_err("should not be ready");
        assert!(error.contains("not ready"));
    }

    #[test]
    fn evaluate_health_fails_when_consecutive_failures_exceed_threshold() {
        let mut progress = base_progress();
        progress.consecutive_failures = 4;
        progress.last_error = Some("upstream timeout".to_string());
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        let error = evaluate_health(&progress, &policy, 0, &peer_health, 0)
            .expect_err("should fail health check");
        assert!(error.contains("consecutive_failures"));
    }

    #[test]
    fn evaluate_health_fails_when_sync_lag_exceeds_threshold() {
        let mut progress = base_progress();
        progress.current_block = 100;
        progress.highest_block = 300;
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        let error = evaluate_health(&progress, &policy, 0, &peer_health, 0)
            .expect_err("should fail health check");
        assert!(error.contains("sync_lag"));
    }

    #[test]
    fn evaluate_health_fails_when_peers_required_and_none_available() {
        let mut progress = base_progress();
        progress.peer_count = 0;
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: true,
        };
        let error = evaluate_health(&progress, &policy, 1, &peer_health, 0)
            .expect_err("missing peers must fail health check");
        assert!(error.contains("peer_count=0"));
    }

    #[test]
    fn evaluate_health_fails_when_peer_count_is_below_minimum_threshold() {
        let mut progress = base_progress();
        progress.peer_count = 1;
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        let error = evaluate_health(&progress, &policy, 2, &peer_health, 0)
            .expect_err("peer count below min threshold must fail health check");
        assert!(error.contains("required minimum=2"));
    }

    #[test]
    fn evaluate_health_fails_when_recent_peer_health_flaps_exceed_threshold() {
        let progress = base_progress();
        let mut peer_health = base_peer_health_status();
        peer_health.flap_transitions_recent = 4;
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };

        let error = evaluate_health(&progress, &policy, 0, &peer_health, 3)
            .expect_err("recent flap transitions above threshold must fail health");
        assert!(error.contains("peer_health_flap_transitions_recent"));
    }

    #[test]
    fn evaluate_health_allows_recent_peer_health_flaps_within_threshold() {
        let progress = base_progress();
        let mut peer_health = base_peer_health_status();
        peer_health.flap_transitions_recent = 3;
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };

        assert!(evaluate_health(&progress, &policy, 0, &peer_health, 3).is_ok());
    }

    #[test]
    fn peer_health_tracker_counts_observations_and_flaps() {
        let mut tracker = PeerHealthTracker::new(1, 16, 300);
        tracker.record(1, 10);
        tracker.record(0, 20);
        tracker.record(3, 30);
        tracker.record(0, 40);

        let status = tracker.snapshot(40);
        assert!(!status.healthy);
        assert_eq!(status.history_size, 4);
        assert_eq!(status.observations_total, 4);
        assert_eq!(status.healthy_observations_total, 2);
        assert_eq!(status.unhealthy_observations_total, 2);
        assert_eq!(status.flap_transitions_total, 3);
        assert_eq!(status.flap_transitions_recent, 3);
        assert_eq!(status.last_observed_at_unix_seconds, Some(40));
        assert_eq!(status.last_peer_count, Some(0));
    }

    #[test]
    fn peer_health_tracker_prunes_history_and_recent_flap_window() {
        let mut tracker = PeerHealthTracker::new(1, 2, 30);
        tracker.record(1, 0);
        tracker.record(0, 10); // flap 1
        tracker.record(2, 20); // flap 2
        tracker.record(0, 70); // flap 3

        let status = tracker.snapshot(70);
        assert_eq!(status.history_size, 2);
        assert_eq!(status.flap_transitions_total, 3);
        assert_eq!(status.flap_transitions_recent, 1);
        assert!(!status.healthy);
        assert_eq!(status.last_peer_count, Some(0));
    }

    #[test]
    fn parse_bootnode_endpoint_supports_socket_and_multiaddr_formats() {
        assert_eq!(
            parse_bootnode_endpoint("127.0.0.1:9090"),
            Some(BootnodeEndpoint::Socket(SocketAddr::from((
                [127, 0, 0, 1],
                9090
            ))))
        );
        assert_eq!(
            parse_bootnode_endpoint("/ip4/127.0.0.1/tcp/9999/p2p/12D3KooWabc"),
            Some(BootnodeEndpoint::Socket(SocketAddr::from((
                [127, 0, 0, 1],
                9999
            ))))
        );
        assert_eq!(
            parse_bootnode_endpoint("/dns4/bootstrap.starknet.io/tcp/30303/p2p/12D3KooWabc"),
            Some(BootnodeEndpoint::HostPort {
                host: "bootstrap.starknet.io".to_string(),
                port: 30_303,
            })
        );
    }

    #[test]
    fn parse_bootnode_endpoints_rejects_invalid_entries() {
        let err = parse_bootnode_endpoints(&["bad-bootnode".to_string()])
            .expect_err("invalid bootnode should fail parsing");
        assert!(err.contains("invalid bootnode"));
    }

    #[test]
    fn parse_bootnode_endpoints_deduplicates_equivalent_entries() {
        let parsed = parse_bootnode_endpoints(&[
            "127.0.0.1:9090".to_string(),
            "/ip4/127.0.0.1/tcp/9090/p2p/12D3KooWabc".to_string(),
            "127.0.0.1:9090".to_string(),
            "bootstrap.starknet.io:30303".to_string(),
            "/dns4/bootstrap.starknet.io/tcp/30303/p2p/12D3KooWdef".to_string(),
        ])
        .expect("equivalent entries should parse");
        assert_eq!(
            parsed,
            vec![
                BootnodeEndpoint::Socket(SocketAddr::from(([127, 0, 0, 1], 9090))),
                BootnodeEndpoint::HostPort {
                    host: "bootstrap.starknet.io".to_string(),
                    port: 30_303
                },
            ]
        );
    }

    #[test]
    fn parse_bootnode_endpoint_rejects_zero_port_and_whitespace_host() {
        assert_eq!(parse_bootnode_endpoint("127.0.0.1:0"), None);
        assert_eq!(parse_bootnode_endpoint("/ip4/127.0.0.1/tcp/0"), None);
        assert_eq!(parse_bootnode_endpoint("bad host:30303"), None);
    }

    #[test]
    fn parse_upstream_rpc_urls_accepts_comma_separated_urls_and_deduplicates() {
        let parsed = parse_upstream_rpc_urls(
            "https://rpc-1.example, https://rpc-2.example:9545, https://rpc-1.example",
        )
        .expect("valid upstream URL list should parse");
        assert_eq!(
            parsed,
            vec![
                "https://rpc-1.example".to_string(),
                "https://rpc-2.example:9545".to_string(),
            ]
        );
    }

    #[test]
    fn parse_upstream_rpc_urls_rejects_excessive_endpoint_count() {
        let raw = (0..=MAX_UPSTREAM_RPC_ENDPOINTS)
            .map(|index| format!("https://rpc-{index}.example"))
            .collect::<Vec<_>>()
            .join(",");
        let error = parse_upstream_rpc_urls(&raw)
            .expect_err("too many upstream endpoints must fail closed");
        assert!(error.contains("max supported"));
    }

    #[test]
    fn parse_upstream_rpc_urls_rejects_oversized_entries() {
        let oversized = format!(
            "https://rpc.example/{}",
            "a".repeat(MAX_UPSTREAM_RPC_URL_BYTES + 1)
        );
        let error = parse_upstream_rpc_urls(&oversized)
            .expect_err("oversized URL entries must fail closed");
        assert!(error.contains("exceeds max"));
    }

    #[test]
    fn parse_upstream_rpc_urls_rejects_control_characters() {
        let error = parse_upstream_rpc_urls("https://rpc.\nexample")
            .expect_err("control chars in URL must fail closed");
        assert!(error.contains("control characters"));
    }

    #[test]
    fn parse_upstream_rpc_urls_error_does_not_echo_secrets() {
        let error = parse_upstream_rpc_urls("https://user:supersecret@")
            .expect_err("malformed URL must fail");
        assert!(!error.contains("supersecret"));
        assert!(error.contains("invalid upstream RPC URL entry"));
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn strict_canonical_defaults_enabled_for_production_builds() {
        assert!(default_strict_canonical_execution());
    }

    #[cfg(not(feature = "production-adapters"))]
    #[test]
    fn strict_canonical_defaults_disabled_without_production_adapters() {
        assert!(!default_strict_canonical_execution());
    }

    #[test]
    fn parse_bool_literal_accepts_common_true_values() {
        for value in ["true", "TRUE", "1", "yes", "on"] {
            assert!(
                parse_bool_literal(value, "test-field").expect("true variant should parse"),
                "value `{value}` should parse as true"
            );
        }
    }

    #[test]
    fn parse_bool_literal_accepts_common_false_values() {
        for value in ["false", "FALSE", "0", "no", "off"] {
            assert!(
                !parse_bool_literal(value, "test-field").expect("false variant should parse"),
                "value `{value}` should parse as false"
            );
        }
    }

    #[test]
    fn parse_bool_literal_rejects_invalid_values() {
        let error =
            parse_bool_literal("maybe", "test-field").expect_err("invalid literal should fail");
        assert!(error.contains("expected one of true/false"));
    }

    #[test]
    fn resolve_strict_canonical_execution_prefers_cli_over_environment_and_default() {
        let resolved = resolve_strict_canonical_execution(Some(false), Some(true), true);
        assert!(!resolved);
    }

    #[test]
    fn resolve_strict_canonical_execution_prefers_environment_over_default() {
        let resolved = resolve_strict_canonical_execution(None, Some(false), true);
        assert!(!resolved);
    }

    #[test]
    fn resolve_strict_canonical_execution_uses_default_when_no_overrides_set() {
        assert!(resolve_strict_canonical_execution(None, None, true));
        assert!(!resolve_strict_canonical_execution(None, None, false));
    }

    #[tokio::test]
    async fn probe_bootnode_detects_reachable_socket() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener bind should succeed");
        let addr = listener.local_addr().expect("listener local addr");
        let endpoint = BootnodeEndpoint::Socket(addr);

        let accept_task = tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let reachable = probe_bootnode(&endpoint, Duration::from_millis(250)).await;
        assert!(reachable);
        let _ = accept_task.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probe_bootnodes_runs_probes_concurrently_with_bounded_fanout() {
        let endpoints = (0..65_u16)
            .map(|port| BootnodeEndpoint::Socket(SocketAddr::from(([127, 0, 0, 1], port))))
            .collect::<Vec<_>>();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let probe = {
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            move |_endpoint: BootnodeEndpoint, timeout: Duration| {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                async move {
                    let in_flight = active.fetch_add(1, Ordering::SeqCst).saturating_add(1);
                    max_active.fetch_max(in_flight, Ordering::SeqCst);
                    tokio::time::sleep(timeout).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    true
                }
            }
        };

        let started = Instant::now();
        let reachable = probe_bootnodes_with(&endpoints, Duration::from_millis(40), &probe).await;
        let elapsed = started.elapsed();

        assert_eq!(reachable, endpoints.len());
        let observed_max = max_active.load(Ordering::SeqCst);
        assert!(
            observed_max > 1 && observed_max <= MAX_BOOTNODE_PROBE_CONCURRENCY,
            "expected bounded concurrency in (1, {}], observed {}",
            MAX_BOOTNODE_PROBE_CONCURRENCY,
            observed_max
        );
        assert!(
            elapsed < Duration::from_millis(350),
            "expected bounded-concurrency probe to finish quickly, elapsed={elapsed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probe_bootnodes_streams_work_without_chunk_barriers() {
        let endpoint_count = MAX_BOOTNODE_PROBE_CONCURRENCY as u16 + 2;
        let base_port = 20_000_u16;
        let endpoints = (0..endpoint_count)
            .map(|idx| {
                BootnodeEndpoint::Socket(SocketAddr::from((
                    [127, 0, 0, 1],
                    base_port.saturating_add(idx),
                )))
            })
            .collect::<Vec<_>>();

        let slow_finished = Arc::new(AtomicBool::new(false));
        let late_started_before_slow_finished = Arc::new(AtomicBool::new(false));
        let probe = {
            let slow_finished = Arc::clone(&slow_finished);
            let late_started_before_slow_finished = Arc::clone(&late_started_before_slow_finished);
            move |endpoint: BootnodeEndpoint, _timeout: Duration| {
                let slow_finished = Arc::clone(&slow_finished);
                let late_started_before_slow_finished =
                    Arc::clone(&late_started_before_slow_finished);
                async move {
                    let BootnodeEndpoint::Socket(addr) = endpoint else {
                        return false;
                    };
                    let index = addr.port().saturating_sub(base_port);
                    if index == 0 {
                        tokio::time::sleep(Duration::from_millis(120)).await;
                        slow_finished.store(true, Ordering::SeqCst);
                        return true;
                    }
                    if index >= MAX_BOOTNODE_PROBE_CONCURRENCY as u16
                        && !slow_finished.load(Ordering::SeqCst)
                    {
                        late_started_before_slow_finished.store(true, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    true
                }
            }
        };

        let reachable = probe_bootnodes_with(&endpoints, Duration::from_millis(1), &probe).await;
        assert_eq!(reachable, endpoints.len());
        assert!(
            late_started_before_slow_finished.load(Ordering::SeqCst),
            "expected post-initial probes to start before the slow first probe completed"
        );
    }

    #[test]
    fn classify_rpc_task_completion_treats_clean_exit_as_fatal() {
        let error = classify_rpc_task_completion(Ok(Ok(())), false)
            .expect_err("clean RPC task exit should fail closed");
        assert!(error.contains("exited unexpectedly"));
    }

    #[test]
    fn classify_rpc_task_completion_allows_clean_exit_when_expected() {
        classify_rpc_task_completion(Ok(Ok(())), true)
            .expect("clean RPC task exit should be allowed after shutdown");
    }

    #[test]
    fn classify_rpc_task_completion_propagates_server_error() {
        let error = classify_rpc_task_completion(Ok(Err("rpc boom".to_string())), true)
            .expect_err("rpc server error should be propagated");
        assert_eq!(error, "rpc boom");
    }

    #[tokio::test]
    async fn join_task_with_timeout_returns_task_output() {
        let mut handle = tokio::spawn(async { 7_u64 });
        let outcome =
            join_task_with_timeout(&mut handle, Duration::from_millis(100), "unit-test-task")
                .await
                .expect("join with timeout should succeed");
        let value = outcome.expect("task should complete successfully");
        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn join_task_with_timeout_reports_timeout_and_aborts() {
        let mut handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            9_u64
        });
        let error = join_task_with_timeout(
            &mut handle,
            Duration::from_millis(5),
            "unit-test-timeout-task",
        )
        .await
        .expect_err("timeout should fail closed");
        assert!(error.contains("exceeded graceful shutdown timeout"));
        assert!(handle.is_finished());
    }

    #[tokio::test]
    async fn wait_for_shutdown_signal_returns_after_send() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let waiter = tokio::spawn(async move {
            wait_for_shutdown_signal(&mut shutdown_rx).await;
        });
        shutdown_tx
            .send(true)
            .expect("shutdown signal send should succeed");
        tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("waiter should complete quickly")
            .expect("waiter task should not panic");
    }

    #[tokio::test]
    async fn bootnode_heartbeat_stops_when_shutdown_requested() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let observation = Arc::new(BootnodeObservation::new(0, unix_now_seconds()));
        let sync_progress = Arc::new(Mutex::new(base_progress()));
        let peer_health_tracker = Arc::new(Mutex::new(PeerHealthTracker::new(
            0,
            DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
            DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
        )));
        let heartbeat = tokio::spawn(run_bootnode_heartbeat(
            Vec::new(),
            0,
            1_000,
            observation,
            sync_progress,
            peer_health_tracker,
            shutdown_rx,
        ));

        shutdown_tx
            .send(true)
            .expect("shutdown signal send should succeed");
        tokio::time::timeout(Duration::from_millis(100), heartbeat)
            .await
            .expect("heartbeat should stop quickly")
            .expect("heartbeat task should not panic");
    }

    #[tokio::test]
    async fn bootnode_heartbeat_waits_one_interval_before_first_probe() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener bind should succeed");
        let addr = listener.local_addr().expect("listener local addr");
        let endpoints = vec![BootnodeEndpoint::Socket(addr)];
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let observation = Arc::new(BootnodeObservation::new(0, unix_now_seconds()));
        let sync_progress = Arc::new(Mutex::new(base_progress()));
        let peer_health_tracker = Arc::new(Mutex::new(PeerHealthTracker::new(
            1,
            DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
            DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
        )));
        let heartbeat = tokio::spawn(run_bootnode_heartbeat(
            endpoints,
            1,
            250,
            observation.clone(),
            sync_progress.clone(),
            Arc::clone(&peer_health_tracker),
            shutdown_rx,
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        let (initial_peers, _) = observation.snapshot();
        assert_eq!(
            initial_peers, 0,
            "heartbeat should not probe before one full interval"
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if observation.snapshot().0 >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("heartbeat should probe after interval");

        let progress = sync_progress
            .lock()
            .expect("sync progress lock should not be poisoned")
            .clone();
        assert_eq!(progress.peer_count, 1);
        let peer_health = peer_health_tracker
            .lock()
            .expect("peer health tracker lock should not be poisoned")
            .snapshot(unix_now_seconds());
        assert!(peer_health.observations_total >= 1);
        assert!(peer_health.healthy);

        shutdown_tx
            .send(true)
            .expect("shutdown signal send should succeed");
        tokio::time::timeout(Duration::from_millis(200), heartbeat)
            .await
            .expect("heartbeat should stop quickly")
            .expect("heartbeat task should not panic");
    }

    #[test]
    fn validate_rpc_auth_token_rejects_whitespace() {
        let err =
            validate_rpc_auth_token("abc def".to_string()).expect_err("whitespace token must fail");
        assert!(err.contains("whitespace"));
    }

    #[test]
    fn validate_rpc_bind_exposure_rejects_public_bind_without_auth() {
        let err = validate_rpc_bind_exposure("0.0.0.0:9545", None, false)
            .expect_err("public bind without auth should fail");
        assert!(err.contains("refusing non-loopback"));
    }

    #[test]
    fn validate_rpc_bind_exposure_allows_public_bind_with_auth() {
        validate_rpc_bind_exposure("0.0.0.0:9545", Some("token"), false)
            .expect("auth token should allow public bind");
    }

    #[test]
    fn validate_rpc_bind_exposure_allows_loopback_without_auth() {
        validate_rpc_bind_exposure("127.0.0.1:9545", None, false)
            .expect("loopback bind should be allowed");
    }

    #[test]
    fn validate_rpc_rate_limit_per_minute_accepts_disabled_and_reasonable_values() {
        assert_eq!(
            validate_rpc_rate_limit_per_minute(0).expect("zero should disable rate limiting"),
            0
        );
        assert_eq!(
            validate_rpc_rate_limit_per_minute(DEFAULT_RPC_RATE_LIMIT_PER_MINUTE)
                .expect("default should be accepted"),
            DEFAULT_RPC_RATE_LIMIT_PER_MINUTE
        );
        assert_eq!(
            validate_rpc_rate_limit_per_minute(MAX_RPC_RATE_LIMIT_PER_MINUTE)
                .expect("upper bound should be accepted"),
            MAX_RPC_RATE_LIMIT_PER_MINUTE
        );
    }

    #[test]
    fn validate_rpc_rate_limit_per_minute_rejects_extreme_values() {
        let err = validate_rpc_rate_limit_per_minute(MAX_RPC_RATE_LIMIT_PER_MINUTE + 1)
            .expect_err("values above cap should fail closed");
        assert!(err.contains("exceeds max"));
    }

    #[test]
    fn rpc_rate_limiter_enforces_limit_for_single_ip() {
        let mut limiter = RpcRateLimiter::new(2);
        let ip = IpAddr::from([127, 0, 0, 1]);
        limiter
            .check_and_record(ip, 1_000)
            .expect("first request should pass");
        limiter
            .check_and_record(ip, 1_001)
            .expect("second request should pass");
        let err = limiter
            .check_and_record(ip, 1_002)
            .expect_err("third request within minute should be rate limited");
        assert!(err.contains("rate limit exceeded"));
    }

    #[test]
    fn rpc_rate_limiter_is_scoped_per_ip() {
        let mut limiter = RpcRateLimiter::new(1);
        let ip_a = IpAddr::from([127, 0, 0, 1]);
        let ip_b = IpAddr::from([127, 0, 0, 2]);
        limiter
            .check_and_record(ip_a, 2_000)
            .expect("first ip should pass");
        limiter
            .check_and_record(ip_b, 2_001)
            .expect("second ip should pass independently");
    }

    #[test]
    fn validate_rpc_bind_exposure_rejects_named_host_without_auth() {
        let err = validate_rpc_bind_exposure("example.com:9545", None, false)
            .expect_err("named host without auth should fail closed");
        assert!(err.contains("refusing non-loopback"));
    }

    #[test]
    fn validate_rpc_bind_exposure_allows_named_host_with_auth() {
        validate_rpc_bind_exposure("example.com:9545", Some("token"), false)
            .expect("named host should be allowed when auth is configured");
    }

    #[test]
    fn validate_bootnode_inputs_rejects_excessive_count() {
        let bootnodes = vec!["127.0.0.1:9090".to_string(); MAX_BOOTNODES + 1];
        let err =
            validate_bootnode_inputs(&bootnodes).expect_err("too many bootnodes must fail closed");
        assert!(err.contains("too many bootnodes"));
    }

    #[test]
    fn validate_bootnode_inputs_rejects_oversized_entry() {
        let oversized = format!("127.0.0.1:{}", "9".repeat(MAX_BOOTNODE_ENTRY_BYTES));
        let err = validate_bootnode_inputs(&[oversized])
            .expect_err("oversized bootnode entry must fail closed");
        assert!(err.contains("exceeds max"));
    }

    #[test]
    fn validate_bootnode_inputs_rejects_control_characters() {
        let err = validate_bootnode_inputs(&["127.0.0.1:9090\nbad".to_string()])
            .expect_err("control characters must fail closed");
        assert!(err.contains("control characters"));
    }

    #[test]
    fn validate_bootnode_inputs_accepts_reasonable_entries() {
        validate_bootnode_inputs(&[
            "127.0.0.1:9090".to_string(),
            "/ip4/127.0.0.1/tcp/9091/p2p/12D3KooWabc".to_string(),
        ])
        .expect("valid bootnodes should be accepted");
    }

    #[test]
    fn validate_peer_requirements_accepts_disabled_or_satisfied_thresholds() {
        validate_peer_requirements(0, 0).expect("min peers disabled should be valid");
        validate_peer_requirements(1, 1).expect("exact threshold should be valid");
        validate_peer_requirements(2, 3).expect("lower-than-bootnode threshold should be valid");
    }

    #[test]
    fn validate_peer_requirements_rejects_threshold_without_bootnodes() {
        let error = validate_peer_requirements(1, 0)
            .expect_err("min peers with zero bootnodes must fail closed");
        assert!(error.contains("no valid bootnodes"));
    }

    #[test]
    fn validate_peer_requirements_rejects_impossible_threshold() {
        let error =
            validate_peer_requirements(3, 2).expect_err("min peers above bootnode count must fail");
        assert!(error.contains("exceeds configured bootnode count"));
    }

    #[test]
    fn unhealthy_exit_reason_is_none_when_exit_disabled() {
        let mut progress = base_progress();
        progress.current_block = 1;
        progress.highest_block = 1_000;
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        assert!(unhealthy_exit_reason(&progress, &policy, 0, false, peer_health, 0).is_none());
    }

    #[test]
    fn unhealthy_exit_reason_reports_failure_when_exit_enabled() {
        let mut progress = base_progress();
        progress.consecutive_failures = 10;
        progress.last_error = Some("upstream timeout".to_string());
        let peer_health = base_peer_health_status();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
            require_peers: false,
        };
        let reason = unhealthy_exit_reason(&progress, &policy, 0, true, peer_health, 0)
            .expect("enabled unhealthy exit should surface reason");
        assert!(reason.contains("consecutive_failures"));
    }

    #[tokio::test]
    async fn handle_rpc_rejects_when_concurrency_limit_reached() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: None,
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let _permit = state
            .rpc_slots
            .clone()
            .try_acquire_owned()
            .expect("should reserve the only slot");
        let response = handle_rpc(
            State(state),
            HeaderMap::new(),
            peer(30_001),
            r#"{"jsonrpc":"2.0","id":1,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn handle_rpc_processes_request_when_slot_is_available() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: None,
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let response = handle_rpc(
            State(state),
            HeaderMap::new(),
            peer(30_002),
            r#"{"jsonrpc":"2.0","id":7,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let payload: Value =
            serde_json::from_slice(&bytes).expect("response body should be valid JSON");
        assert_eq!(payload["error"]["code"], serde_json::json!(32));
        assert_eq!(payload["id"], serde_json::json!(7));
    }

    #[tokio::test]
    async fn handle_rpc_rejects_missing_bearer_token_when_required() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: Some("supersecret".to_string()),
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let response = handle_rpc(
            State(state),
            HeaderMap::new(),
            peer(30_003),
            r#"{"jsonrpc":"2.0","id":99,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn handle_rpc_accepts_valid_bearer_token_when_required() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: Some("supersecret".to_string()),
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer supersecret"),
        );
        let response = handle_rpc(
            State(state),
            headers,
            peer(30_004),
            r#"{"jsonrpc":"2.0","id":100,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn handle_rpc_enforces_per_ip_rate_limit() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: None,
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(1))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let first = handle_rpc(
            State(state.clone()),
            HeaderMap::new(),
            peer(30_005),
            r#"{"jsonrpc":"2.0","id":111,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);

        let second = handle_rpc(
            State(state),
            HeaderMap::new(),
            peer(30_005),
            r#"{"jsonrpc":"2.0","id":112,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn metrics_reports_rpc_counters_after_successful_request() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: None,
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let response = handle_rpc(
            State(state.clone()),
            HeaderMap::new(),
            peer(30_006),
            r#"{"jsonrpc":"2.0","id":113,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let metrics_response = metrics(State(state), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(metrics_response.status(), StatusCode::OK);
        let body = to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .expect("metrics response body should be readable");
        let text =
            String::from_utf8(body.to_vec()).expect("metrics payload should be valid UTF-8 text");
        assert!(text.contains("pastis_rpc_requests_total 1"));
        assert!(text.contains("pastis_rpc_responses_ok_total 1"));
        assert!(text.contains("pastis_rpc_responses_no_content_total 0"));
        assert!(text.contains("pastis_sync_current_block 10"));
        assert!(text.contains("pastis_sync_min_peer_count 0"));
        assert!(text.contains("pastis_peer_health_max_recent_flaps 0"));
        assert!(text.contains("pastis_peer_health_status 1"));
        assert!(text.contains("pastis_peer_health_observations_total 0"));
        assert!(text.contains("pastis_peer_health_flap_transitions_total 0"));
        assert!(text.contains("pastis_peer_health_last_peer_count 0"));
    }

    #[tokio::test]
    async fn metrics_reports_notification_counter_for_no_content_responses() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: None,
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let response = handle_rpc(
            State(state.clone()),
            HeaderMap::new(),
            peer(30_007),
            r#"{"jsonrpc":"2.0","method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let metrics_response = metrics(State(state), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(metrics_response.status(), StatusCode::OK);
        let body = to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .expect("metrics response body should be readable");
        let text =
            String::from_utf8(body.to_vec()).expect("metrics payload should be valid UTF-8 text");
        assert!(text.contains("pastis_rpc_requests_total 1"));
        assert!(text.contains("pastis_rpc_responses_ok_total 0"));
        assert!(text.contains("pastis_rpc_responses_no_content_total 1"));
        assert!(text.contains("pastis_peer_health_history_size 0"));
    }

    #[tokio::test]
    async fn metrics_rejects_missing_bearer_token_when_required() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: Some("supersecret".to_string()),
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let response = metrics(State(state), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn metrics_accepts_valid_bearer_token_when_required() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: Some("supersecret".to_string()),
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer supersecret"),
        );
        let response = metrics(State(state), headers).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn status_rejects_missing_bearer_token_when_required() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: Some("supersecret".to_string()),
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let response = status(State(state), HeaderMap::new())
            .await
            .expect_err("missing token should fail");
        assert_eq!(response.0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn status_accepts_valid_bearer_token_when_required() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let state = RpcAppState {
            storage,
            chain_id: "SN_MAIN".to_string(),
            rpc_auth_token: Some("supersecret".to_string()),
            sync_progress: Arc::new(Mutex::new(base_progress())),
            health_policy: HealthPolicy {
                max_consecutive_failures: 3,
                max_sync_lag: 64,
                require_peers: false,
            },
            min_peer_count: 0,
            max_recent_peer_health_flaps: 0,
            peer_health_tracker: Arc::new(Mutex::new(PeerHealthTracker::new(
                0,
                DEFAULT_PEER_HEALTH_HISTORY_CAPACITY,
                DEFAULT_PEER_HEALTH_FLAP_WINDOW_SECS,
            ))),
            rpc_slots: Arc::new(Semaphore::new(1)),
            rpc_rate_limiter: Arc::new(Mutex::new(RpcRateLimiter::new(0))),
            rpc_metrics: Arc::new(Mutex::new(RpcRuntimeMetrics::default())),
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer supersecret"),
        );
        let payload = status(State(state), headers)
            .await
            .expect("valid token should allow status access");
        assert_eq!(payload.0.chain_id, "SN_MAIN");
        assert_eq!(payload.0.current_block, 10);
        assert_eq!(payload.0.min_peers, 0);
        assert_eq!(payload.0.max_recent_peer_health_flaps, 0);
        assert!(payload.0.peer_health.healthy);
        assert_eq!(payload.0.peer_health.last_peer_count, None);
        assert_eq!(payload.0.peer_health.history_size, 0);
        assert_eq!(payload.0.peer_health.observations_total, 0);
    }
}
