use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::time::interval;

use starknet_node::ChainId;
use starknet_node::runtime::{
    DEFAULT_MAX_REPLAY_PER_POLL, DEFAULT_REPLAY_WINDOW, DEFAULT_RPC_MAX_RETRIES,
    DEFAULT_RPC_RETRY_BACKOFF_MS, DEFAULT_RPC_TIMEOUT_SECS, DEFAULT_SYNC_POLL_MS, NodeRuntime,
    RpcRetryConfig, RuntimeConfig, SyncProgress,
};
use starknet_node_rpc::{StarknetRpcServer, SyncStatus};
use starknet_node_storage::{InMemoryStorage, ThreadSafeStorage};

const DEFAULT_RPC_BIND: &str = "127.0.0.1:9545";
const DEFAULT_REPLAY_CHECKPOINT_PATH: &str = ".pastis/node-replay-checkpoint.json";
const DEFAULT_LOCAL_JOURNAL_PATH: &str = ".pastis/node-local-journal.jsonl";
const DEFAULT_P2P_HEARTBEAT_MS: u64 = 30_000;

#[derive(Debug, Clone)]
struct DaemonConfig {
    upstream_rpc_url: String,
    rpc_bind: String,
    chain_id: ChainId,
    poll_ms: u64,
    replay_window: u64,
    max_replay_per_poll: u64,
    replay_checkpoint_path: Option<String>,
    local_journal_path: Option<String>,
    rpc_timeout_secs: u64,
    rpc_max_retries: u32,
    rpc_retry_backoff_ms: u64,
    bootnodes: Vec<String>,
    require_peers: bool,
    p2p_heartbeat_ms: u64,
}

#[derive(Clone)]
struct RpcAppState {
    storage: ThreadSafeStorage<InMemoryStorage>,
    chain_id: String,
    sync_progress: Arc<Mutex<SyncProgress>>,
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

#[tokio::main]
async fn main() -> Result<(), String> {
    let config = parse_daemon_config()?;

    let runtime_config = RuntimeConfig {
        chain_id: config.chain_id.clone(),
        upstream_rpc_url: config.upstream_rpc_url.clone(),
        replay_window: config.replay_window,
        max_replay_per_poll: config.max_replay_per_poll,
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
        sync_progress,
    };
    let app = Router::new()
        .route("/", post(handle_rpc))
        .route("/healthz", get(healthz))
        .route("/status", get(status))
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
    if let Some(path) = &config.local_journal_path {
        println!("local_journal_path: {path}");
    }
    println!("require_peers: {}", config.require_peers);

    let rpc_handle = tokio::spawn(async move {
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
            _ = tokio::signal::ctrl_c() => {
                println!("received shutdown signal");
                break;
            }
            _ = ticker.tick() => {
                if let Err(error) = runtime.poll_once().await {
                    eprintln!("warning: sync poll failed: {error}");
                }
            }
        }
    }

    rpc_handle.abort();
    match rpc_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => eprintln!("warning: rpc server exited with error: {error}"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => eprintln!("warning: rpc server task join error: {error}"),
    }

    Ok(())
}

async fn handle_rpc(State(state): State<RpcAppState>, raw: String) -> impl IntoResponse {
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

async fn healthz() -> impl IntoResponse {
    StatusCode::OK
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
    let mut cli_chain_id: Option<ChainId> = None;
    let mut cli_poll_ms: Option<u64> = None;
    let mut cli_replay_window: Option<u64> = None;
    let mut cli_max_replay_per_poll: Option<u64> = None;
    let mut cli_replay_checkpoint_path: Option<String> = None;
    let mut cli_local_journal_path: Option<String> = None;
    let mut cli_rpc_timeout_secs: Option<u64> = None;
    let mut cli_rpc_max_retries: Option<u32> = None;
    let mut cli_rpc_retry_backoff_ms: Option<u64> = None;
    let mut cli_bootnodes: Vec<String> = Vec::new();
    let mut cli_require_peers = false;
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
            "--bootnode" => {
                cli_bootnodes.push(
                    args.next()
                        .ok_or_else(|| "--bootnode requires a value".to_string())?,
                );
            }
            "--require-peers" => {
                cli_require_peers = true;
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

    let chain_id = match cli_chain_id {
        Some(id) => id,
        None => {
            let raw = env::var("PASTIS_CHAIN_ID").unwrap_or_else(|_| "SN_MAIN".to_string());
            ChainId::parse(raw).map_err(|error| format!("invalid PASTIS_CHAIN_ID: {error}"))?
        }
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
    let p2p_heartbeat_ms = match cli_p2p_heartbeat_ms {
        Some(value) => value,
        None => parse_env_u64("PASTIS_P2P_HEARTBEAT_MS")?.unwrap_or(DEFAULT_P2P_HEARTBEAT_MS),
    };
    let require_peers = if cli_require_peers {
        true
    } else {
        parse_env_bool("PASTIS_REQUIRE_PEERS")?.unwrap_or(false)
    };

    Ok(DaemonConfig {
        upstream_rpc_url,
        rpc_bind: cli_rpc_bind
            .or_else(|| env::var("PASTIS_NODE_RPC_BIND").ok())
            .unwrap_or_else(|| DEFAULT_RPC_BIND.to_string()),
        chain_id,
        poll_ms,
        replay_window,
        max_replay_per_poll,
        replay_checkpoint_path: cli_replay_checkpoint_path
            .or_else(|| env::var("PASTIS_REPLAY_CHECKPOINT_PATH").ok())
            .or_else(|| Some(DEFAULT_REPLAY_CHECKPOINT_PATH.to_string())),
        local_journal_path: cli_local_journal_path
            .or_else(|| env::var("PASTIS_LOCAL_JOURNAL_PATH").ok())
            .or_else(|| Some(DEFAULT_LOCAL_JOURNAL_PATH.to_string())),
        rpc_timeout_secs,
        rpc_max_retries,
        rpc_retry_backoff_ms,
        bootnodes,
        require_peers,
        p2p_heartbeat_ms,
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

fn help_text() -> String {
    format!(
        "usage: starknet-node --upstream-rpc-url <url> [options]\n\
options:\n\
  --rpc-bind <addr>                  JSON-RPC bind address (default: {DEFAULT_RPC_BIND})\n\
  --chain-id <id>                    Chain id (default: SN_MAIN)\n\
  --poll-ms <ms>                     Sync poll interval in milliseconds\n\
  --replay-window <blocks>           Replay window size\n\
  --max-replay-per-poll <blocks>     Max blocks to process per poll\n\
  --replay-checkpoint <path>         Replay checkpoint file path\n\
  --local-journal <path>             Local block/state journal path\n\
  --rpc-timeout-secs <secs>          Upstream RPC timeout\n\
  --rpc-max-retries <n>              Upstream RPC max retries\n\
  --rpc-retry-backoff-ms <ms>        Retry backoff base\n\
  --bootnode <multiaddr>             Configure bootnode (repeatable)\n\
  --require-peers                    Fail closed when no peers are configured/available\n\
  --p2p-heartbeat-ms <ms>            P2P heartbeat logging interval\n\
environment:\n\
  STARKNET_RPC_URL                   Upstream Starknet RPC URL\n\
  PASTIS_NODE_RPC_BIND               Local JSON-RPC bind\n\
  PASTIS_CHAIN_ID                    Node chain id\n\
  PASTIS_NODE_POLL_MS                Sync poll interval\n\
  PASTIS_REPLAY_WINDOW               Replay window\n\
  PASTIS_MAX_REPLAY_PER_POLL         Max replay per poll\n\
  PASTIS_REPLAY_CHECKPOINT_PATH      Replay checkpoint path\n\
  PASTIS_LOCAL_JOURNAL_PATH          Local block/state journal path\n\
  PASTIS_RPC_TIMEOUT_SECS            Upstream RPC timeout seconds\n\
  PASTIS_RPC_MAX_RETRIES             Upstream RPC max retries\n\
  PASTIS_RPC_RETRY_BACKOFF_MS        Upstream RPC retry backoff ms\n\
  PASTIS_BOOTNODES                   Comma-separated bootnodes\n\
  PASTIS_REQUIRE_PEERS               Require peers for sync loop health (true/false)\n\
  PASTIS_P2P_HEARTBEAT_MS            P2P heartbeat interval"
    )
}
