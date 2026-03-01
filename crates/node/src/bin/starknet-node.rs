use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::interval;

use starknet_node::ChainId;
use starknet_node::runtime::{
    DEFAULT_CHAIN_ID_REVALIDATE_POLLS, DEFAULT_MAX_REPLAY_PER_POLL, DEFAULT_REPLAY_WINDOW,
    DEFAULT_RPC_MAX_RETRIES, DEFAULT_RPC_RETRY_BACKOFF_MS, DEFAULT_RPC_TIMEOUT_SECS,
    DEFAULT_SYNC_POLL_MS, NodeRuntime, RpcRetryConfig, RuntimeConfig, SyncProgress,
};
use starknet_node_rpc::{StarknetRpcServer, SyncStatus};
use starknet_node_storage::{InMemoryStorage, ThreadSafeStorage};

const DEFAULT_RPC_BIND: &str = "127.0.0.1:9545";
const DEFAULT_REPLAY_CHECKPOINT_PATH: &str = ".pastis/node-replay-checkpoint.json";
const DEFAULT_LOCAL_JOURNAL_PATH: &str = ".pastis/node-local-journal.jsonl";
const DEFAULT_P2P_HEARTBEAT_MS: u64 = 30_000;
const DEFAULT_RPC_MAX_CONCURRENCY: usize = 256;
const DEFAULT_HEALTH_MAX_CONSECUTIVE_FAILURES: u64 = 3;
const DEFAULT_HEALTH_MAX_SYNC_LAG_BLOCKS: u64 = 64;
const MAX_RPC_AUTH_TOKEN_BYTES: usize = 4 * 1024;

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
    rpc_timeout_secs: u64,
    rpc_max_retries: u32,
    rpc_retry_backoff_ms: u64,
    rpc_max_concurrency: usize,
    bootnodes: Vec<String>,
    require_peers: bool,
    p2p_heartbeat_ms: u64,
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
    rpc_slots: Arc<Semaphore>,
}

#[derive(Debug, Clone, Serialize)]
struct StatusPayload {
    chain_id: String,
    starting_block: u64,
    current_block: u64,
    highest_block: u64,
    peer_count: u64,
    reorg_events: u64,
    consecutive_failures: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct HealthPolicy {
    max_consecutive_failures: u64,
    max_sync_lag: u64,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let config = parse_daemon_config()?;

    let runtime_config = RuntimeConfig {
        chain_id: config.chain_id.clone(),
        upstream_rpc_url: config.upstream_rpc_url.clone(),
        replay_window: config.replay_window,
        max_replay_per_poll: config.max_replay_per_poll,
        chain_id_revalidate_polls: config.chain_id_revalidate_polls,
        replay_checkpoint_path: config.replay_checkpoint_path.clone(),
        local_journal_path: config.local_journal_path.clone(),
        poll_interval: Duration::from_millis(config.poll_ms),
        rpc_timeout: Duration::from_secs(config.rpc_timeout_secs),
        retry: RpcRetryConfig {
            max_retries: config.rpc_max_retries,
            base_backoff: Duration::from_millis(config.rpc_retry_backoff_ms),
        },
        peer_count_hint: config.bootnodes.len(),
        require_peers: config.require_peers,
    };

    let mut runtime = NodeRuntime::new(runtime_config)?;
    let storage = runtime.storage();
    let sync_progress = runtime.sync_progress_handle();
    let chain_id = runtime.chain_id().to_string();

    if !config.bootnodes.is_empty() {
        let bootnodes = config.bootnodes.clone();
        let heartbeat_ms = config.p2p_heartbeat_ms.max(1_000);
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(heartbeat_ms));
            loop {
                ticker.tick().await;
                eprintln!(
                    "p2p heartbeat: {} configured bootnodes, sync loop active",
                    bootnodes.len()
                );
            }
        });
    }

    let app_state = RpcAppState {
        storage,
        chain_id: chain_id.clone(),
        rpc_auth_token: config.rpc_auth_token.clone(),
        sync_progress,
        health_policy: HealthPolicy {
            max_consecutive_failures: config.health_max_consecutive_failures,
            max_sync_lag: config.health_max_sync_lag_blocks,
        },
        rpc_slots: Arc::new(Semaphore::new(config.rpc_max_concurrency)),
    };
    let app = Router::new()
        .route("/", post(handle_rpc))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(app_state);

    let listener = TcpListener::bind(&config.rpc_bind)
        .await
        .map_err(|error| format!("failed to bind RPC endpoint {}: {error}", config.rpc_bind))?;

    println!("starknet-node daemon starting");
    println!("chain_id: {chain_id}");
    println!(
        "upstream_rpc_url: {}",
        redact_rpc_url(&config.upstream_rpc_url)
    );
    println!("rpc_bind: {}", config.rpc_bind);
    println!("poll_ms: {}", config.poll_ms);
    println!(
        "chain_id_revalidate_polls: {}",
        config.chain_id_revalidate_polls
    );
    println!("rpc_max_concurrency: {}", config.rpc_max_concurrency);
    println!("rpc_auth_enabled: {}", config.rpc_auth_token.is_some());
    println!("allow_public_rpc_bind: {}", config.allow_public_rpc_bind);
    println!("exit_on_unhealthy: {}", config.exit_on_unhealthy);
    if let Some(path) = &config.local_journal_path {
        println!("local_journal_path: {path}");
    }
    println!("require_peers: {}", config.require_peers);

    let mut rpc_handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .map_err(|error| format!("rpc server failed: {error}"))
    });

    if let Err(error) = runtime.poll_once().await {
        eprintln!("warning: initial sync poll failed: {error}");
    }

    let mut ticker = interval(runtime.poll_interval());
    ticker.tick().await;

    loop {
        tokio::select! {
            rpc_outcome = &mut rpc_handle => {
                return classify_rpc_task_completion(rpc_outcome);
            }
            _ = tokio::signal::ctrl_c() => {
                println!("received shutdown signal");
                break;
            }
            _ = ticker.tick() => {
                if let Err(error) = runtime.poll_once().await {
                    eprintln!("warning: sync poll failed: {error}");
                }
                let progress = runtime
                    .sync_progress_handle()
                    .lock()
                    .map_err(|_| "sync progress lock poisoned".to_string())?
                    .clone();
                if let Some(reason) = unhealthy_exit_reason(
                    &progress,
                    &HealthPolicy {
                        max_consecutive_failures: config.health_max_consecutive_failures,
                        max_sync_lag: config.health_max_sync_lag_blocks,
                    },
                    config.exit_on_unhealthy,
                ) {
                    return Err(format!(
                        "fatal health condition while sync loop is active: {reason}"
                    ));
                }
            }
        }
    }

    if !rpc_handle.is_finished() {
        rpc_handle.abort();
    }
    match rpc_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => eprintln!("warning: rpc server exited with error: {error}"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => eprintln!("warning: rpc server task join error: {error}"),
    }

    Ok(())
}

async fn handle_rpc(
    State(state): State<RpcAppState>,
    headers: HeaderMap,
    raw: String,
) -> impl IntoResponse {
    if !is_rpc_request_authorized(&headers, state.rpc_auth_token.as_deref()) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "missing or invalid bearer token".to_string(),
        )
            .into_response();
    }

    let _rpc_slot = match state.rpc_slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
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
        return StatusCode::NO_CONTENT.into_response();
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        response,
    )
        .into_response()
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

    match evaluate_health(&progress, &state.health_policy) {
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

    match evaluate_readiness(&progress, &state.health_policy) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(message) => (StatusCode::SERVICE_UNAVAILABLE, message).into_response(),
    }
}

async fn status(
    State(state): State<RpcAppState>,
) -> Result<Json<StatusPayload>, (StatusCode, String)> {
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

    Ok(Json(StatusPayload {
        chain_id: state.chain_id,
        starting_block: progress.starting_block,
        current_block: progress.current_block,
        highest_block: progress.highest_block,
        peer_count: progress.peer_count,
        reorg_events: progress.reorg_events,
        consecutive_failures: progress.consecutive_failures,
        last_error: progress.last_error,
    }))
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
    let mut cli_rpc_timeout_secs: Option<u64> = None;
    let mut cli_rpc_max_retries: Option<u32> = None;
    let mut cli_rpc_retry_backoff_ms: Option<u64> = None;
    let mut cli_rpc_max_concurrency: Option<usize> = None;
    let mut cli_bootnodes: Vec<String> = Vec::new();
    let mut cli_require_peers = false;
    let mut cli_exit_on_unhealthy = false;
    let mut cli_p2p_heartbeat_ms: Option<u64> = None;

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
            "--help" | "-h" => {
                return Err(help_text());
            }
            unknown => {
                return Err(format!("unknown flag `{unknown}`\n\n{}", help_text()));
            }
        }
    }

    let upstream_rpc_url = cli_upstream_rpc
        .or_else(|| env::var("STARKNET_RPC_URL").ok())
        .ok_or_else(|| {
            "missing upstream RPC URL; pass --upstream-rpc-url or set STARKNET_RPC_URL".to_string()
        })?;
    reqwest::Url::parse(&upstream_rpc_url)
        .map_err(|error| format!("invalid upstream RPC URL `{upstream_rpc_url}`: {error}"))?;

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
    let p2p_heartbeat_ms = match cli_p2p_heartbeat_ms {
        Some(value) => value,
        None => parse_env_u64("PASTIS_P2P_HEARTBEAT_MS")?.unwrap_or(DEFAULT_P2P_HEARTBEAT_MS),
    };
    let health_max_consecutive_failures = parse_env_u64("PASTIS_HEALTH_MAX_CONSECUTIVE_FAILURES")?
        .unwrap_or(DEFAULT_HEALTH_MAX_CONSECUTIVE_FAILURES);
    let health_max_sync_lag_blocks = parse_env_u64("PASTIS_HEALTH_MAX_SYNC_LAG_BLOCKS")?
        .unwrap_or(DEFAULT_HEALTH_MAX_SYNC_LAG_BLOCKS);
    let require_peers = if cli_require_peers {
        true
    } else {
        parse_env_bool("PASTIS_REQUIRE_PEERS")?.unwrap_or(false)
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
        rpc_timeout_secs,
        rpc_max_retries,
        rpc_retry_backoff_ms,
        rpc_max_concurrency,
        bootnodes,
        require_peers,
        p2p_heartbeat_ms,
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
        Ok(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Ok(Some(true)),
                "0" | "false" | "no" | "off" => Ok(Some(false)),
                _ => Err(format!(
                    "invalid {name} value `{raw}`: expected one of true/false/1/0/yes/no/on/off"
                )),
            }
        }
        Err(_) => Ok(None),
    }
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

fn parse_positive_usize(raw: &str, field: &str) -> Result<usize, String> {
    let parsed = raw
        .parse::<usize>()
        .map_err(|error| format!("invalid {field} value `{raw}`: {error}"))?;
    if parsed == 0 {
        return Err(format!("invalid {field} value `{raw}`: must be > 0"));
    }
    Ok(parsed)
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

fn evaluate_health(progress: &SyncProgress, policy: &HealthPolicy) -> Result<(), String> {
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

    Ok(())
}

fn evaluate_readiness(progress: &SyncProgress, policy: &HealthPolicy) -> Result<(), String> {
    evaluate_health(progress, policy)?;
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
) -> Result<(), String> {
    match outcome {
        Ok(Ok(())) => Err("rpc server exited unexpectedly".to_string()),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(format!("rpc server task join error: {error}")),
    }
}

fn unhealthy_exit_reason(
    progress: &SyncProgress,
    policy: &HealthPolicy,
    exit_on_unhealthy: bool,
) -> Option<String> {
    if !exit_on_unhealthy {
        return None;
    }
    evaluate_health(progress, policy).err()
}

fn help_text() -> String {
    format!(
        "usage: starknet-node --upstream-rpc-url <url> [options]\n\
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
  --rpc-timeout-secs <secs>          Upstream RPC timeout\n\
  --rpc-max-retries <n>              Upstream RPC max retries\n\
  --rpc-retry-backoff-ms <ms>        Retry backoff base\n\
  --rpc-max-concurrency <n>          Max concurrent local RPC requests (default: {DEFAULT_RPC_MAX_CONCURRENCY})\n\
  --bootnode <multiaddr>             Configure bootnode (repeatable)\n\
  --require-peers                    Fail closed when no peers are configured/available\n\
  --exit-on-unhealthy                Exit daemon when health checks fail\n\
  --p2p-heartbeat-ms <ms>            P2P heartbeat logging interval\n\
environment:\n\
  STARKNET_RPC_URL                   Upstream Starknet RPC URL\n\
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
  PASTIS_RPC_TIMEOUT_SECS            Upstream RPC timeout seconds\n\
  PASTIS_RPC_MAX_RETRIES             Upstream RPC max retries\n\
  PASTIS_RPC_RETRY_BACKOFF_MS        Upstream RPC retry backoff ms\n\
  PASTIS_RPC_MAX_CONCURRENCY         Max concurrent local RPC requests\n\
  PASTIS_BOOTNODES                   Comma-separated bootnodes\n\
  PASTIS_REQUIRE_PEERS               Require peers for sync loop health (true/false)\n\
  PASTIS_EXIT_ON_UNHEALTHY           Exit daemon when health checks fail (true/false)\n\
  PASTIS_P2P_HEARTBEAT_MS            P2P heartbeat interval\n\
  PASTIS_HEALTH_MAX_CONSECUTIVE_FAILURES   Health failure threshold for consecutive poll failures\n\
  PASTIS_HEALTH_MAX_SYNC_LAG_BLOCKS         Health failure threshold for sync lag in blocks"
    )
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http::HeaderValue;
    use serde_json::Value;
    use tokio::sync::Semaphore;

    use starknet_node_storage::{InMemoryStorage, ThreadSafeStorage};
    use starknet_node_types::InMemoryState;

    use super::*;

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

    #[test]
    fn evaluate_health_reports_healthy_when_within_thresholds() {
        let progress = base_progress();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
        };
        assert!(evaluate_health(&progress, &policy).is_ok());
    }

    #[test]
    fn evaluate_readiness_is_ready_when_synced_and_healthy() {
        let progress = base_progress();
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
        };
        assert!(evaluate_readiness(&progress, &policy).is_ok());
    }

    #[test]
    fn evaluate_readiness_fails_when_node_is_still_catching_up() {
        let mut progress = base_progress();
        progress.current_block = 10;
        progress.highest_block = 20;
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
        };
        let error = evaluate_readiness(&progress, &policy).expect_err("should not be ready");
        assert!(error.contains("not ready"));
    }

    #[test]
    fn evaluate_health_fails_when_consecutive_failures_exceed_threshold() {
        let mut progress = base_progress();
        progress.consecutive_failures = 4;
        progress.last_error = Some("upstream timeout".to_string());
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
        };
        let error = evaluate_health(&progress, &policy).expect_err("should fail health check");
        assert!(error.contains("consecutive_failures"));
    }

    #[test]
    fn evaluate_health_fails_when_sync_lag_exceeds_threshold() {
        let mut progress = base_progress();
        progress.current_block = 100;
        progress.highest_block = 300;
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
        };
        let error = evaluate_health(&progress, &policy).expect_err("should fail health check");
        assert!(error.contains("sync_lag"));
    }

    #[test]
    fn classify_rpc_task_completion_treats_clean_exit_as_fatal() {
        let error = classify_rpc_task_completion(Ok(Ok(())))
            .expect_err("clean RPC task exit should fail closed");
        assert!(error.contains("exited unexpectedly"));
    }

    #[test]
    fn classify_rpc_task_completion_propagates_server_error() {
        let error = classify_rpc_task_completion(Ok(Err("rpc boom".to_string())))
            .expect_err("rpc server error should be propagated");
        assert_eq!(error, "rpc boom");
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
    fn unhealthy_exit_reason_is_none_when_exit_disabled() {
        let mut progress = base_progress();
        progress.current_block = 1;
        progress.highest_block = 1_000;
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
        };
        assert!(unhealthy_exit_reason(&progress, &policy, false).is_none());
    }

    #[test]
    fn unhealthy_exit_reason_reports_failure_when_exit_enabled() {
        let mut progress = base_progress();
        progress.consecutive_failures = 10;
        progress.last_error = Some("upstream timeout".to_string());
        let policy = HealthPolicy {
            max_consecutive_failures: 3,
            max_sync_lag: 64,
        };
        let reason = unhealthy_exit_reason(&progress, &policy, true)
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
            },
            rpc_slots: Arc::new(Semaphore::new(1)),
        };

        let _permit = state
            .rpc_slots
            .clone()
            .try_acquire_owned()
            .expect("should reserve the only slot");
        let response = handle_rpc(
            State(state),
            HeaderMap::new(),
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
            },
            rpc_slots: Arc::new(Semaphore::new(1)),
        };

        let response = handle_rpc(
            State(state),
            HeaderMap::new(),
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
        assert_eq!(payload["result"], serde_json::json!(0));
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
            },
            rpc_slots: Arc::new(Semaphore::new(1)),
        };

        let response = handle_rpc(
            State(state),
            HeaderMap::new(),
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
            },
            rpc_slots: Arc::new(Semaphore::new(1)),
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer supersecret"),
        );
        let response = handle_rpc(
            State(state),
            headers,
            r#"{"jsonrpc":"2.0","id":100,"method":"starknet_blockNumber","params":[]}"#.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
