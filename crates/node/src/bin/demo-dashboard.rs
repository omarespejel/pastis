#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

const DEMO_API_KEY: &str = "boss-demo-key";
const WBTC_CONTRACT: &str = "0x111";
const STRKBTC_SHIELDED_POOL: &str = "0x222";
const STRKBTC_MERKLE_ROOT_KEY: &str = "0x10";
const STRKBTC_COMMITMENT_COUNT_KEY: &str = "0x11";
const STRKBTC_NULLIFIER_COUNT_KEY: &str = "0x12";
const STRKBTC_NULLIFIER_PREFIX: &str = "0xdead";

#[derive(Clone)]
struct AppState {
    runtime: Arc<Mutex<DemoRuntime>>,
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

#[derive(Debug, Serialize)]
struct StatusPayload {
    chain_id: String,
    latest_block: u64,
    state_root: String,
    mcp_roundtrip_ok: bool,
    recent_anomaly_count: usize,
}

#[derive(Debug, Serialize)]
struct AnomaliesPayload {
    anomalies: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(Mutex::new(DemoRuntime::new()));
    let simulation_runtime = Arc::clone(&runtime);
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(2));
        loop {
            ticker.tick().await;
            if let Ok(mut guard) = simulation_runtime.lock()
                && let Err(error) = guard.tick()
            {
                eprintln!("warning: demo runtime tick failed: {error}");
            }
        }
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/anomalies", get(anomalies))
        .with_state(AppState { runtime });

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("demo dashboard: http://127.0.0.1:8080");
    println!("mcp demo api key: {DEMO_API_KEY}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn status(
    State(state): State<AppState>,
) -> Result<Json<StatusPayload>, (StatusCode, String)> {
    let guard = state.runtime.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "runtime lock poisoned".to_string(),
        )
    })?;
    let latest_block = guard.node.storage.latest_block_number().map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("latest block failed: {error}"),
        )
    })?;
    let state_root = guard.node.storage.current_state_root().map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("state root failed: {error}"),
        )
    })?;
    let anomalies = guard
        .query_anomalies_via_mcp(25)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(StatusPayload {
        chain_id: guard.node.config.chain_id.as_str().to_string(),
        latest_block,
        state_root,
        mcp_roundtrip_ok: true,
        recent_anomaly_count: anomalies.len(),
    }))
}

async fn anomalies(
    State(state): State<AppState>,
) -> Result<Json<AnomaliesPayload>, (StatusCode, String)> {
    let guard = state.runtime.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "runtime lock poisoned".to_string(),
        )
    })?;
    let anomalies = guard
        .query_anomalies_via_mcp(25)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(AnomaliesPayload {
        anomalies: anomalies
            .into_iter()
            .map(|anomaly| format!("{anomaly:?}"))
            .collect(),
    }))
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
  <title>Pastis Demo Dashboard</title>
  <style>
    :root {
      --bg: #f7f5ef;
      --panel: #ffffff;
      --ink: #111827;
      --muted: #6b7280;
      --accent: #0f766e;
      --warn: #b45309;
      --danger: #b91c1c;
      --border: #d1d5db;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      --sans: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
    }
    body {
      margin: 0;
      background: radial-gradient(1200px 500px at 85% -20%, #d9f99d 0%, transparent 55%), var(--bg);
      color: var(--ink);
      font-family: var(--sans);
    }
    .wrap {
      max-width: 1080px;
      margin: 0 auto;
      padding: 24px;
    }
    .title {
      margin: 0 0 8px 0;
      font-size: 32px;
      letter-spacing: 0.2px;
    }
    .subtitle {
      margin: 0 0 24px 0;
      color: var(--muted);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
      box-shadow: 0 4px 10px rgba(17, 24, 39, 0.05);
    }
    .card h3 {
      margin: 0 0 8px 0;
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .value {
      font-size: 26px;
      line-height: 1.2;
      font-weight: 650;
    }
    .ok { color: var(--accent); }
    .warn { color: var(--warn); }
    .anomalies {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
    }
    .anomalies h2 {
      margin: 0 0 12px 0;
      font-size: 18px;
    }
    .list {
      list-style: none;
      margin: 0;
      padding: 0;
      display: grid;
      gap: 8px;
    }
    .item {
      border: 1px solid #e5e7eb;
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
    @media (max-width: 600px) {
      .value { font-size: 22px; }
      .title { font-size: 28px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1 class="title">Pastis Live Demo</h1>
    <p class="subtitle">Simulated Starknet node flow with MCP anomaly querying and BTCFi monitor output.</p>

    <div class="grid">
      <div class="card"><h3>Latest Block</h3><div class="value" id="latestBlock">-</div></div>
      <div class="card"><h3>State Root</h3><div class="value" style="font-family:var(--mono);font-size:15px" id="stateRoot">-</div></div>
      <div class="card"><h3>Chain ID</h3><div class="value" id="chainId">-</div></div>
      <div class="card"><h3>MCP Roundtrip</h3><div class="value ok" id="mcpStatus">-</div></div>
      <div class="card"><h3>Recent Anomalies</h3><div class="value warn" id="anomalyCount">-</div></div>
    </div>

    <section class="anomalies">
      <h2>Recent BTCFi/strkBTC Anomalies (via MCP `GetAnomalies`)</h2>
      <ul class="list" id="anomalyList"></ul>
      <div class="meta" id="meta"></div>
    </section>
  </div>

  <script>
    async function load() {
      const [statusRes, anomaliesRes] = await Promise.all([
        fetch('/api/status'),
        fetch('/api/anomalies'),
      ]);
      if (!statusRes.ok || !anomaliesRes.ok) {
        document.getElementById('meta').textContent = 'Failed to load demo data';
        return;
      }

      const status = await statusRes.json();
      const anomalies = await anomaliesRes.json();

      document.getElementById('latestBlock').textContent = status.latest_block;
      document.getElementById('stateRoot').textContent = status.state_root;
      document.getElementById('chainId').textContent = status.chain_id;
      document.getElementById('mcpStatus').textContent = status.mcp_roundtrip_ok ? 'OK' : 'FAIL';
      document.getElementById('mcpStatus').className = 'value ' + (status.mcp_roundtrip_ok ? 'ok' : 'warn');
      document.getElementById('anomalyCount').textContent = status.recent_anomaly_count;

      const list = document.getElementById('anomalyList');
      list.innerHTML = '';
      if (!anomalies.anomalies.length) {
        const li = document.createElement('li');
        li.className = 'item';
        li.textContent = 'No anomalies yet. Wait a few blocks.';
        list.appendChild(li);
      } else {
        for (const item of anomalies.anomalies) {
          const li = document.createElement('li');
          li.className = 'item';
          li.textContent = item;
          list.appendChild(li);
        }
      }
      document.getElementById('meta').textContent = 'Auto-refresh every 2s';
    }

    load();
    setInterval(load, 2000);
  </script>
</body>
</html>
"#;
