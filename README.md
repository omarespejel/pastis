# Pastis

Pastis is an AI-first Starknet full node architecture, implemented in Rust, with a hard line between:

- `Foundation`: storage, sync, L1 finality tracking, health surfaces
- `Execution`: canonical and fast execution backends with explicit mismatch policy
- `Agent`: ExEx-style post-execution consumers (telemetry, MCP, BTCFi monitoring)

The project is designed for production safety first: fail-closed versioning, bounded queues, WAL recovery, deterministic delivery ordering, and explicit degradation paths.

## Why This Exists

Starknet has had real sequencer/prover mismatch incidents. Pastis treats those as first-class architecture constraints:

- Fast execution is allowed.
- Canonical verification is required where correctness matters.
- Any mismatch policy is explicit, observable, and testable.

The goal is a node architecture that is fast enough for operators and strict enough for auditors.

## Current State

This repository currently ships core, tested building blocks:

- Versioned ExEx notifications with backward-compatible WAL decode
- ExEx delivery planning with dependencies, priorities, cycle detection, and depth limits
- L1 finality tracker with reorg invalidation and contiguous verified-block progression
- Protocol-version constant resolution with fail-closed behavior
- Storage backends with Apollo adapter tests against real Starknet fixture data
- Dual execution orchestration with canonical fallback/mismatch handling
- Production node wiring now injects an Apollo-backed Blockifier class provider with
  block-aware pre-state binding for account-transaction execution paths
- ExEx manager with bounded WAL replay and failing-sink circuit breaker
- OTel-style ExEx sink with deterministic per-block metrics capture and regression counters
- BTCFi/strkBTC ExEx scaffolding with structural anomaly detection primitives and bounded nullifier-tracking state
- Proving-layer scaffolding with verify-only backend and trace-driven proving pipeline
- MCP server request handler with authenticated tool execution, bounded batch support, and BTCFi anomaly query surface
- JSON-RPC server request handler with strict JSON-RPC 2.0 validation and Starknet core method coverage
- Type-state node builder that enforces composition order

Source entry points:

- [crates/node-spec-core/src/notification.rs](./crates/node-spec-core/src/notification.rs)
- [crates/node-spec-core/src/exex_delivery.rs](./crates/node-spec-core/src/exex_delivery.rs)
- [crates/node-spec-core/src/l1_finality.rs](./crates/node-spec-core/src/l1_finality.rs)
- [crates/node-spec-core/src/protocol_version.rs](./crates/node-spec-core/src/protocol_version.rs)
- [crates/storage/src/lib.rs](./crates/storage/src/lib.rs)
- [crates/execution/src/lib.rs](./crates/execution/src/lib.rs)
- [crates/exex-manager/src/lib.rs](./crates/exex-manager/src/lib.rs)
- [crates/exex-otel/src/lib.rs](./crates/exex-otel/src/lib.rs)
- [crates/exex-btcfi/src/lib.rs](./crates/exex-btcfi/src/lib.rs)
- [crates/proving/src/lib.rs](./crates/proving/src/lib.rs)
- [crates/mcp-server/src/lib.rs](./crates/mcp-server/src/lib.rs)
- [crates/rpc/src/lib.rs](./crates/rpc/src/lib.rs)
- [crates/node/src/lib.rs](./crates/node/src/lib.rs)

## External Integration Boundaries

Planned integrations with `starknet-agentic`, `starkclaw`, and `SISNA` are treated as untrusted-by-default external systems.

- Trust boundary:
  - External agents never receive direct mutable handles to storage, consensus, or execution internals.
  - Integration must happen through MCP tools and ExEx notifications only.
- Authentication and authorization:
  - Every MCP request must authenticate with an agent-scoped key.
  - Tool access is permission-gated per agent policy and rate limited.
  - Agent identity is derived from credential verification, not caller-provided IDs.
- Input validation:
  - All batch MCP requests are bounded by maximum depth and maximum size.
  - Total flattened tool count across nested batches is bounded.
  - Unknown tools, malformed payloads, and non-monotonic control-plane timestamps are rejected.
- Operational isolation:
  - In-process ExEx execution is trusted-only and requires per-ExEx credentials.
  - ExExes are logically isolated by bounded queues, circuit breakers, and cooldown-based sink recovery.
  - Third-party/untrusted agent logic must run out-of-process (container/process/WASM boundary) with least-privilege credentials.

## Chain ID Policy

`NodeConfig.chain_id` accepts:

- `SN_MAIN`
- `SN_SEPOLIA`
- custom IDs with strict format `SN_[A-Z0-9_]+`

Custom IDs are intentionally supported for private networks/appchains and test environments.

## Architecture Narrative

1. A block is received and validated by consensus/sync components.
2. Execution runs through a selected backend mode:
   - `CanonicalOnly`
   - `FastOnly`
   - `DualWithVerification`
3. State is committed via storage backend behavior (Apollo storage for canonical Starknet semantics).
4. RPC requests are handled synchronously against committed storage state.
5. ExEx notifications are WAL-persisted before delivery.
6. ExEx manager delivers notifications in deterministic dependency tiers.
7. Agent-facing surfaces (e.g., MCP) consume notifications and read state without mutating consensus-critical paths.

## Local Development

### Prerequisites

- Rust toolchain (stable)
- `cargo`

### Boss Demo Dashboard

Run a live local dashboard that simulates block flow, drives BTCFi anomaly generation, and queries anomalies through MCP:

```bash
cargo run -p starknet-node --bin demo-dashboard --all-features -- --mode demo
```

Run against a real Starknet RPC endpoint:

```bash
STARKNET_RPC_URL="https://<your-starknet-rpc>" \
cargo run -p starknet-node --bin demo-dashboard --all-features -- --mode real
```

Real mode persists replay cursor state by default at `.pastis/replay-checkpoint.json` so restart recovery is deterministic.
Override with `--replay-checkpoint <path>` or `PASTIS_DASHBOARD_REPLAY_CHECKPOINT=<path>`.

Enable live BTCFi anomaly processing in `real` mode (state-diff driven) by setting monitor env vars:

```bash
PASTIS_MONITOR_STRKBTC_SHIELDED_POOL="0x<shielded-pool-contract>" \
PASTIS_MONITOR_WBTC_CONTRACT="0x<wbtc-contract>" \
STARKNET_RPC_URL="https://<your-starknet-rpc>" \
cargo run -p starknet-node --bin demo-dashboard --all-features -- --mode real
```

When enabled, the dashboard reports anomaly source as `mcp:get_anomalies(local)` and `/api/debug` includes
`last_processed_block`, `last_local_block`, `commit_failure_count`, and retained anomaly counters.

Open:

- `http://127.0.0.1:8080/` for the dashboard UI
- `http://127.0.0.1:8080/api/status` for live node status JSON
- `http://127.0.0.1:8080/api/anomalies` for live anomaly JSON
- `http://127.0.0.1:8080/api/debug` for runtime diagnostics (last errors, failure counters, snapshot availability)

What this demo proves on-screen:

- Node builder wiring (`storage + execution + rpc + mcp`)
- Block ingestion and committed-state progression (`demo` mode)
- Live chain status pull from Starknet JSON-RPC (`real` mode)
- Real-mode `starknet_getStateUpdate` conversion into internal `StarknetStateDiff`
- Optional real-mode BTCFi anomaly processing (`btcfi:state-diff` source)
- BTCFi/strkBTC anomaly detector behavior
- MCP tool round-trip via `GetAnomalies`

### Validate Everything

```bash
cargo fmt --all --check
cargo test --workspace
cargo test -p starknet-node-storage --features apollo-adapter
cargo test -p starknet-node-execution --features blockifier-adapter
cargo clippy --workspace --all-targets --all-features -- -D warnings
./scripts/check-storage-adapter-deps.sh
./scripts/check-replay-fixture-lock.sh
```

Refresh replay RPC fixtures (block + state update pairs) and lockfile:

```bash
./scripts/capture-replay-rpc-fixtures.sh --rpc-url "https://<your-starknet-rpc>"
```

## Performance Gate (Next Step)

The project now includes an internal performance budget harness (debug-mode regression guard) to catch severe overhead regressions in critical control-plane operations.

Run it locally:

```bash
cargo run -p starknet-node --bin perf-budget
```

Tune limits with environment variables:

- `PASTIS_PERF_ITERATIONS`
- `PASTIS_BUDGET_DUAL_EXECUTE_P99_US`
- `PASTIS_BUDGET_NOTIFICATION_DECODE_P99_US`
- `PASTIS_BUDGET_MCP_VALIDATE_P99_US`
- `PASTIS_BUDGET_DUAL_EXECUTE_MIN_OPS_PER_SEC`

## Roadmap

Near-term implementation sequence:

1. Expand Apollo-backed adapters and fixture coverage
2. Expand Apollo class-provider fixture coverage with declared-class/CASM artifacts and
   account-transaction replay assertions
3. Extend RPC method coverage and map full Starknet error taxonomy
4. Extend MCP server surface beyond control-plane tools (state query params, simulation, submit)
5. Wire BTCFi ExEx into ExExManager registration flow
6. Attach concrete trace providers/provers to the proving pipeline for end-to-end proof jobs

Long-term:

- Full production node binary wiring
- End-to-end sync pipeline integration
- Production-grade STWO proving backend integration
