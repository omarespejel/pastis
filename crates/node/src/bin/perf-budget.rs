use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use bincode::Options;
use node_spec_core::mcp::{McpTool, ValidationLimits, validate_tool};
use node_spec_core::notification::{NotificationV2, StarknetExExNotification, decode_wal_entry};
use semver::Version;
use starknet_node_execution::{
    DualExecutionBackend, ExecutionBackend, ExecutionError, ExecutionMode, MismatchPolicy,
};
use starknet_node_types::{
    BlockContext, BlockGasPrices, BuiltinStats, ExecutionOutput, GasPricePerToken, InMemoryState,
    MutableState, SimulationResult, StarknetBlock, StarknetReceipt, StarknetStateDiff,
    StarknetTransaction, StateReader,
};

const DEFAULT_ITERATIONS: usize = 5_000;
const DEFAULT_DUAL_EXECUTE_P99_US: u64 = 5_000;
const DEFAULT_NOTIFICATION_DECODE_P99_US: u64 = 1_000;
const DEFAULT_MCP_VALIDATE_P99_US: u64 = 1_000;
const DEFAULT_DUAL_EXECUTE_MIN_OPS_PER_SEC: f64 = 1_000.0;

#[derive(Debug, Clone, Copy)]
struct PerfBudgets {
    dual_execute_p99_us: u64,
    notification_decode_p99_us: u64,
    mcp_validate_p99_us: u64,
    dual_execute_min_ops_per_sec: f64,
}

impl PerfBudgets {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            dual_execute_p99_us: env_u64(
                "PASTIS_BUDGET_DUAL_EXECUTE_P99_US",
                DEFAULT_DUAL_EXECUTE_P99_US,
            )?,
            notification_decode_p99_us: env_u64(
                "PASTIS_BUDGET_NOTIFICATION_DECODE_P99_US",
                DEFAULT_NOTIFICATION_DECODE_P99_US,
            )?,
            mcp_validate_p99_us: env_u64(
                "PASTIS_BUDGET_MCP_VALIDATE_P99_US",
                DEFAULT_MCP_VALIDATE_P99_US,
            )?,
            dual_execute_min_ops_per_sec: env_f64(
                "PASTIS_BUDGET_DUAL_EXECUTE_MIN_OPS_PER_SEC",
                DEFAULT_DUAL_EXECUTE_MIN_OPS_PER_SEC,
            )?,
        })
    }
}

#[derive(Debug, Clone)]
struct MetricResult {
    name: &'static str,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    ops_per_sec: f64,
}

#[derive(Default)]
struct NoopBackend;

impl ExecutionBackend for NoopBackend {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        _state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        Ok(ExecutionOutput {
            receipts: vec![StarknetReceipt {
                tx_hash: block
                    .transactions
                    .first()
                    .map(|tx| tx.hash.clone())
                    .unwrap_or_else(|| "0x0".to_string()),
                execution_status: true,
                events: 0,
                gas_consumed: 1,
            }],
            state_diff: StarknetStateDiff::default(),
            builtin_stats: BuiltinStats::default(),
            execution_time: Duration::from_micros(1),
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

fn sample_block(number: u64) -> StarknetBlock {
    StarknetBlock {
        number,
        parent_hash: format!("0x{:x}", number.saturating_sub(1)),
        state_root: format!("0x{number:x}"),
        timestamp: 1_700_000_000 + number,
        sequencer_address: "0x1".to_string(),
        gas_prices: BlockGasPrices {
            l1_gas: GasPricePerToken {
                price_in_fri: 2,
                price_in_wei: 3,
            },
            l1_data_gas: GasPricePerToken {
                price_in_fri: 4,
                price_in_wei: 5,
            },
            l2_gas: GasPricePerToken {
                price_in_fri: 6,
                price_in_wei: 7,
            },
        },
        protocol_version: Version::new(0, 14, 2),
        transactions: vec![StarknetTransaction::new(format!("0x{number:x}"))],
    }
}

fn run_dual_execution_benchmark(iterations: usize) -> Result<MetricResult, String> {
    let backend = DualExecutionBackend::new(
        Some(Box::new(NoopBackend)),
        Box::new(NoopBackend),
        ExecutionMode::DualWithVerification {
            verification_depth: 64,
        },
        MismatchPolicy::Halt,
    );
    backend
        .set_verification_tip(iterations as u64 + 1)
        .map_err(|error| error.to_string())?;

    let mut state = InMemoryState::default();
    let mut samples_us = Vec::with_capacity(iterations);
    let started = Instant::now();
    for idx in 0..iterations {
        let block = sample_block(idx as u64 + 1);
        let t0 = Instant::now();
        let out = backend
            .execute_verified(&block, &mut state)
            .map_err(|error| error.to_string())?;
        black_box(out);
        samples_us.push(t0.elapsed().as_micros() as u64);
    }

    Ok(build_metric(
        "dual_execute_verified",
        samples_us,
        started.elapsed(),
    ))
}

fn run_notification_decode_benchmark(iterations: usize) -> Result<MetricResult, String> {
    let payload = StarknetExExNotification::V2(NotificationV2 {
        block_number: 42,
        tx_count: 10,
        event_count: 20,
    });
    let encoded = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .serialize(&payload)
        .map_err(|error| format!("serialize notification: {error}"))?;

    let mut samples_us = Vec::with_capacity(iterations);
    let started = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        let decoded = decode_wal_entry(&encoded).map_err(|error| error.to_string())?;
        black_box(decoded);
        samples_us.push(t0.elapsed().as_micros() as u64);
    }

    Ok(build_metric(
        "notification_decode",
        samples_us,
        started.elapsed(),
    ))
}

fn run_mcp_validation_benchmark(iterations: usize) -> Result<MetricResult, String> {
    let tool = McpTool::BatchQuery {
        queries: vec![McpTool::QueryState, McpTool::GetNodeStatus],
    };
    let limits = ValidationLimits {
        max_batch_size: 32,
        max_depth: 1,
    };

    let mut samples_us = Vec::with_capacity(iterations);
    let started = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        validate_tool(&tool, limits).map_err(|error| error.to_string())?;
        samples_us.push(t0.elapsed().as_micros() as u64);
    }

    Ok(build_metric("mcp_validate", samples_us, started.elapsed()))
}

fn build_metric(name: &'static str, mut samples_us: Vec<u64>, elapsed: Duration) -> MetricResult {
    samples_us.sort_unstable();
    let len = samples_us.len().max(1);
    let p50_us = percentile(&samples_us, 50);
    let p95_us = percentile(&samples_us, 95);
    let p99_us = percentile(&samples_us, 99);
    let ops_per_sec = len as f64 / elapsed.as_secs_f64().max(1e-9);
    MetricResult {
        name,
        p50_us,
        p95_us,
        p99_us,
        ops_per_sec,
    }
}

fn percentile(sorted_samples: &[u64], pct: usize) -> u64 {
    if sorted_samples.is_empty() {
        return 0;
    }
    let n = sorted_samples.len();
    let rank = ((n * pct).div_ceil(100)).max(1);
    sorted_samples[rank - 1]
}

fn env_usize(key: &str, default: usize) -> Result<usize, String> {
    match env::var(key) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("invalid {key}='{value}': {error}")),
        Err(_) => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64, String> {
    match env::var(key) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|error| format!("invalid {key}='{value}': {error}")),
        Err(_) => Ok(default),
    }
}

fn env_f64(key: &str, default: f64) -> Result<f64, String> {
    match env::var(key) {
        Ok(value) => value
            .parse::<f64>()
            .map_err(|error| format!("invalid {key}='{value}': {error}")),
        Err(_) => Ok(default),
    }
}

fn print_metric(metric: &MetricResult) {
    println!(
        "{:<26} p50={}us p95={}us p99={}us ops/s={:.1}",
        metric.name, metric.p50_us, metric.p95_us, metric.p99_us, metric.ops_per_sec
    );
}

fn main() {
    let run = || -> Result<(), String> {
        let iterations = env_usize("PASTIS_PERF_ITERATIONS", DEFAULT_ITERATIONS)?;
        let budgets = PerfBudgets::from_env()?;

        println!("pastis perf budget run (iterations={iterations})");
        let dual = run_dual_execution_benchmark(iterations)?;
        let decode = run_notification_decode_benchmark(iterations)?;
        let mcp = run_mcp_validation_benchmark(iterations)?;

        print_metric(&dual);
        print_metric(&decode);
        print_metric(&mcp);

        let mut failures = Vec::new();
        if dual.p99_us > budgets.dual_execute_p99_us {
            failures.push(format!(
                "dual_execute_verified p99 {}us > budget {}us",
                dual.p99_us, budgets.dual_execute_p99_us
            ));
        }
        if dual.ops_per_sec < budgets.dual_execute_min_ops_per_sec {
            failures.push(format!(
                "dual_execute_verified ops/s {:.1} < budget {:.1}",
                dual.ops_per_sec, budgets.dual_execute_min_ops_per_sec
            ));
        }
        if decode.p99_us > budgets.notification_decode_p99_us {
            failures.push(format!(
                "notification_decode p99 {}us > budget {}us",
                decode.p99_us, budgets.notification_decode_p99_us
            ));
        }
        if mcp.p99_us > budgets.mcp_validate_p99_us {
            failures.push(format!(
                "mcp_validate p99 {}us > budget {}us",
                mcp.p99_us, budgets.mcp_validate_p99_us
            ));
        }

        if failures.is_empty() {
            println!("perf budgets: PASS");
            return Ok(());
        }

        eprintln!("perf budgets: FAIL");
        for failure in failures {
            eprintln!("  - {failure}");
        }
        Err("performance budget regression detected".to_string())
    };

    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_handles_empty_and_bounds() {
        assert_eq!(percentile(&[], 99), 0);
        assert_eq!(percentile(&[10], 99), 10);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 50), 3);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 99), 5);
    }
}
