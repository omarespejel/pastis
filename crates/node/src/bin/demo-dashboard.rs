#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::env;
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
use starknet_node::{NodeConfig, StarknetNode, StarknetNodeBuilder};
use starknet_node_execution::{ExecutionBackend, ExecutionError};
use starknet_node_exex_btcfi::{
    BtcfiAnomaly, BtcfiExEx, StandardWrapper, StandardWrapperConfig, StandardWrapperMonitor,
    StrkBtcMonitor, StrkBtcMonitorConfig,
};
use starknet_node_mcp_server::{McpRequest, McpResponse};
use starknet_node_storage::{InMemoryStorage, StorageBackend};
use starknet_node_types::{
    BlockContext, BlockGasPrices, BuiltinStats, ContractAddress, ExecutionOutput, GasPricePerToken,
    InMemoryState, MutableState, SimulationResult, StarknetBlock, StarknetReceipt,
    StarknetStateDiff, StarknetTransaction, StateReader,
};
use tokio::net::TcpListener;
use tokio::time::interval;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_REFRESH_MS: u64 = 2_000;
const DEMO_API_KEY: &str = "boss-demo-key";

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
}

#[derive(Clone)]
struct AppState {
    source: DashboardSource,
    refresh_ms: u64,
}

#[derive(Clone)]
enum DashboardSource {
    Demo { runtime: Arc<Mutex<DemoRuntime>> },
    Real { client: Arc<RealRpcClient> },
}

struct DemoExecution;

impl ExecutionBackend for DemoExecution {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        _state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        Ok(ExecutionOutput {
            receipts: vec![StarknetReceipt {
                tx_hash: format!("0x{:x}", block.number).into(),
                execution_status: true,
                events: 0,
                gas_consumed: 1,
            }],
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
    node: StarknetNode<InMemoryStorage, DemoExecution>,
    btcfi: Mutex<BtcfiExEx>,
    next_block: u64,
    commitment_count: u64,
    nullifier_count: u64,
    merkle_root: u64,
    last_nullifier_key: Option<String>,
}

impl DemoRuntime {
    fn new() -> Self {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig::default())
            .with_storage(storage)
            .with_execution(DemoExecution)
            .with_rpc(true)
            .with_mcp(true)
            .build();

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
            btcfi: Mutex::new(BtcfiExEx::new(vec![wrapper], strkbtc, 256)),
            next_block: 1,
            commitment_count: 0,
            nullifier_count: 0,
            merkle_root: 0x100,
            last_nullifier_key: None,
        }
    }

    fn tick(&mut self) -> Result<(), String> {
        let block_number = self.next_block;
        let block = demo_block(block_number);
        let diff = self.build_state_diff(block_number);

        self.node
            .storage
            .insert_block(block, diff.clone())
            .map_err(|error| format!("insert block failed: {error}"))?;
        if let Ok(mut btcfi) = self.btcfi.lock() {
            let _ = btcfi.process_block(block_number, &diff);
        }
        self.next_block = self.next_block.saturating_add(1);
        Ok(())
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

#[derive(Clone)]
struct RealRpcClient {
    http: reqwest::Client,
    rpc_url: String,
}

#[derive(Debug)]
struct RealSnapshot {
    chain_id: String,
    latest_block: u64,
    state_root: String,
    tx_count: u64,
    rpc_latency_ms: u64,
}

impl RealRpcClient {
    fn new(rpc_url: String) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .map_err(|error| format!("failed to build HTTP client: {error}"))?;
        Ok(Self { http, rpc_url })
    }

    async fn fetch_status(&self) -> Result<RealSnapshot, String> {
        let started = Instant::now();

        let chain_id_raw = self.call("starknet_chainId", json!([])).await?;
        let chain_id = chain_id_raw
            .as_str()
            .map(std::string::ToString::to_string)
            .unwrap_or_else(|| chain_id_raw.to_string());

        let block_head = self.call("starknet_blockHashAndNumber", json!([])).await?;
        let latest_block = value_as_u64(block_head.get("block_number").unwrap_or(&Value::Null))
            .ok_or_else(|| {
                format!("starknet_blockHashAndNumber missing block_number: {block_head}")
            })?;

        let block_selector = json!([{ "block_number": latest_block }]);
        let tx_count_raw = self
            .call("starknet_getBlockTransactionCount", block_selector.clone())
            .await?;
        let tx_count = value_as_u64(&tx_count_raw)
            .ok_or_else(|| format!("invalid tx count payload: {tx_count_raw}"))?;

        let state_update = self.call("starknet_getStateUpdate", block_selector).await?;
        let state_root = state_update
            .get("new_root")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();

        Ok(RealSnapshot {
            chain_id,
            latest_block,
            state_root,
            tx_count,
            rpc_latency_ms: saturating_u128_to_u64(started.elapsed().as_millis()),
        })
    }

    async fn fetch_anomalies(&self, limit: usize) -> Result<Vec<String>, String> {
        let mut notes = vec![format!(
            "Real mode connected to {}. No MCP anomaly source configured yet.",
            self.rpc_url
        )];
        if limit > 1 {
            notes.push(
                "Connect this dashboard to a Pastis MCP anomaly endpoint for live alerts."
                    .to_string(),
            );
        }
        notes.truncate(limit);
        Ok(notes)
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
    state_root: String,
    tx_count: u64,
    rpc_latency_ms: u64,
    mcp_roundtrip_ok: bool,
    anomaly_source: String,
    recent_anomaly_count: usize,
    refresh_ms: u64,
}

#[derive(Debug, Serialize)]
struct AnomaliesPayload {
    source: String,
    anomalies: Vec<String>,
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
            let client = RealRpcClient::new(rpc_url)?;
            DashboardSource::Real {
                client: Arc::new(client),
            }
        }
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/anomalies", get(anomalies))
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
        DashboardSource::Real { client } => real_status(client, state.refresh_ms).await,
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
        DashboardSource::Real { client } => real_anomalies(client).await,
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
    let anomalies = guard.query_anomalies_via_mcp(25)?;
    Ok(StatusPayload {
        mode: DashboardModeKind::Demo.as_str().to_string(),
        chain_id: guard.node.config.chain_id.as_str().to_string(),
        latest_block,
        state_root,
        tx_count: 1,
        rpc_latency_ms: saturating_u128_to_u64(started.elapsed().as_millis()),
        mcp_roundtrip_ok: true,
        anomaly_source: "mcp:get_anomalies".to_string(),
        recent_anomaly_count: anomalies.len(),
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

async fn real_status(
    client: &Arc<RealRpcClient>,
    refresh_ms: u64,
) -> Result<StatusPayload, String> {
    let snapshot = client.fetch_status().await?;
    let anomalies = client.fetch_anomalies(1).await?;
    Ok(StatusPayload {
        mode: DashboardModeKind::Real.as_str().to_string(),
        chain_id: snapshot.chain_id,
        latest_block: snapshot.latest_block,
        state_root: snapshot.state_root,
        tx_count: snapshot.tx_count,
        rpc_latency_ms: snapshot.rpc_latency_ms,
        mcp_roundtrip_ok: false,
        anomaly_source: "unconfigured".to_string(),
        recent_anomaly_count: anomalies.len(),
        refresh_ms,
    })
}

async fn real_anomalies(client: &Arc<RealRpcClient>) -> Result<AnomaliesPayload, String> {
    let anomalies = client.fetch_anomalies(25).await?;
    Ok(AnomaliesPayload {
        source: "unconfigured".to_string(),
        anomalies,
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
        transactions: Vec::new(),
    }
}

fn parse_dashboard_config() -> Result<DashboardConfig, String> {
    let env_mode = env::var("PASTIS_DASHBOARD_MODE")
        .ok()
        .as_deref()
        .map(DashboardModeKind::parse)
        .transpose()?
        .unwrap_or(DashboardModeKind::Demo);

    let env_refresh = env::var("PASTIS_DASHBOARD_REFRESH_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_REFRESH_MS);
    let env_bind =
        env::var("PASTIS_DASHBOARD_BIND").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
    let env_rpc = env::var("STARKNET_RPC_URL").ok();

    let mut config = DashboardConfig {
        mode: env_mode,
        bind_addr: env_bind,
        refresh_ms: env_refresh,
        rpc_url: env_rpc,
    };

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--mode requires `demo` or `real`".to_string())?;
                config.mode = DashboardModeKind::parse(&raw)?;
            }
            "--rpc-url" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--rpc-url requires a value".to_string())?;
                config.rpc_url = Some(raw);
            }
            "--bind" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--bind requires a value".to_string())?;
                config.bind_addr = raw;
            }
            "--refresh-ms" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--refresh-ms requires a value".to_string())?;
                config.refresh_ms = raw
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --refresh-ms value `{raw}`: {error}"))?;
            }
            "--help" | "-h" => {
                return Err(help_text());
            }
            unknown => {
                return Err(format!("unknown flag `{unknown}`\n\n{}", help_text()));
            }
        }
    }

    if config.mode == DashboardModeKind::Real && config.rpc_url.is_none() {
        return Err(
            "real mode requires --rpc-url <url> or STARKNET_RPC_URL environment variable"
                .to_string(),
        );
    }
    Ok(config)
}

fn help_text() -> String {
    "usage: demo-dashboard [--mode demo|real] [--rpc-url <url>] [--bind <addr>] [--refresh-ms <ms>]
environment:
  PASTIS_DASHBOARD_MODE=demo|real
  STARKNET_RPC_URL=https://...
  PASTIS_DASHBOARD_BIND=127.0.0.1:8080
  PASTIS_DASHBOARD_REFRESH_MS=2000"
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
      <div class="card"><h3>Block Tx Count</h3><div class="value" id="txCount">-</div></div>
      <div class="card"><h3>RPC Latency (ms)</h3><div class="value" id="rpcLatency">-</div></div>
      <div class="card"><h3>Chain ID</h3><div class="value mono" id="chainId">-</div></div>
      <div class="card"><h3>State Root</h3><div class="value mono" id="stateRoot">-</div></div>
      <div class="card"><h3>MCP Roundtrip</h3><div class="value" id="mcpStatus">-</div></div>
      <div class="card"><h3>Anomaly Source</h3><div class="value mono" id="anomalySource">-</div></div>
      <div class="card"><h3>Recent Anomalies</h3><div class="value warn" id="anomalyCount">-</div></div>
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
      const [statusRes, anomaliesRes] = await Promise.all([
        fetch('/api/status'),
        fetch('/api/anomalies'),
      ]);
      if (!statusRes.ok || !anomaliesRes.ok) {
        document.getElementById('meta').textContent = 'Failed to load dashboard data';
        return;
      }

      const status = await statusRes.json();
      const anomalies = await anomaliesRes.json();

      blockHistory.push(status.latest_block);
      if (blockHistory.length > 24) {
        blockHistory.shift();
      }
      renderSparkline();

      document.getElementById('modeBadge').textContent = String(status.mode || '-').toUpperCase();
      document.getElementById('modeBadge').className = status.mode === 'real' ? 'ok' : 'warn';
      document.getElementById('latestBlock').textContent = status.latest_block;
      document.getElementById('txCount').textContent = status.tx_count;
      document.getElementById('rpcLatency').textContent = status.rpc_latency_ms;
      document.getElementById('chainId').textContent = status.chain_id;
      document.getElementById('stateRoot').textContent = status.state_root;
      document.getElementById('anomalySource').textContent = status.anomaly_source;
      document.getElementById('anomalyCount').textContent = status.recent_anomaly_count;

      const mcp = document.getElementById('mcpStatus');
      if (status.mcp_roundtrip_ok) {
        mcp.textContent = 'OK';
        mcp.className = 'value ok';
      } else {
        mcp.textContent = status.mode === 'real' ? 'N/A' : 'FAIL';
        mcp.className = 'value ' + (status.mode === 'real' ? 'warn' : 'danger');
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
      document.getElementById('meta').textContent =
        `Refreshed in ${fetchMs}ms | API refresh=${status.refresh_ms}ms | anomaly source=${anomalies.source}`;
    }

    load();
    setInterval(load, 2000);
  </script>
</body>
</html>
"#;
