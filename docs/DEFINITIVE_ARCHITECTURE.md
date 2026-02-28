# Definitive Architecture: AI-First Starknet Full Node

## 1. System Overview

A Starknet full node built in Rust, following a composable-node pattern adapted for Cairo/Starknet. Three layers:
- Foundation (storage, consensus, P2P, L1 monitoring)
- Execution (dual-backend transaction processing, mempool, RPC)
- Agent (ExExes, MCP server, AI agents)

An ExEx Manager routes post-execution notifications using lightweight, cloneable payloads.

### Design Principles

1. Cairo Native is an optimization, not a foundation.
2. No `unsafe` in the notification path.
3. The node is the tool; agents are operators.
4. strkBTC monitoring is a flagship use case.

## 2. Foundation Layer

### 2.1 Type System

The core type system intentionally excludes prover-only data (`CairoTrace`, proof internals) from standard notifications.

### 2.2 Storage

Apollo Storage is the storage backend of record. State root and trie logic are delegated to Apollo rather than reimplemented.

### 2.3 Consensus and P2P

Consensus backend and network backend are abstracted behind traits. Sync supports full and checkpoint modes.

### 2.4 L1 Monitor

L1 monitor tracks state root posts, proof verification, and forced transactions.

### 2.5 Health Checking

Every foundation and execution component implements a uniform health surface.

## 3. Execution Layer

### 3.1 Dual Backend Roles

- Cairo Native: fast execution outputs.
- BlockifierVM: canonical execution + trace-capable execution.

### 3.2 DualExecutionBackend

Supports `CanonicalOnly`, `FastOnly`, and `DualWithVerification` modes with mismatch policies:
- `WarnAndFallback`
- `Halt`
- `CooldownThenRetry`

Initialization failures of Cairo Native must degrade gracefully to canonical mode.

### 3.3 Trace Provider

Trace generation is explicit and on-demand. It is not in the hot-path notification payload.

### 3.4 Proving Backend

Proving is optional and separated from block execution hot path.

### 3.5 Mempool and RPC

Mempool supports fee estimation, simulation, propagation, and pending-state reads. RPC is synchronous and independent from ExEx asynchronous delivery.

## 4. Agent Layer

### 4.1 Notification Contract

Notifications carry block, state diff, receipts, and summary metadata. They are lightweight and WAL-backed.

### 4.2 ExEx Manager

ExEx manager uses bounded buffering, persistence, and backpressure semantics.

### 4.3 Built-in ExExes

- OTel ExEx for telemetry
- BTCFi ExEx including strkBTC structural monitoring
- MCP server ExEx for tool access

## 5. Security Boundaries

- ExExes cannot mutate consensus state.
- Notification path forbids `unsafe` via crate-level `#![forbid(unsafe_code)]`.
- Agent auto-actions are whitelisted.
- Dual execution mismatch handling is explicit and configurable.
- WAL is written before delivery.

Enforcement note:
- `unsafe` prohibition is compile-time enforced in first-party crates.
- In-process ExEx execution is explicitly `trusted-only`; untrusted ExEx registrations are rejected by policy.
- Untrusted third-party logic must run out-of-process (WASM or process/container isolation) and interact through authenticated MCP/notification surfaces.

## 6. Hardening Clauses (P1/P2 Gaps Closed)

### 6.1 WAL Notification Versioning

WAL notifications are explicitly versioned:

```rust
pub enum StarknetExExNotification {
    V1(NotificationV1),
    V2(NotificationV2),
}
```

Compatibility rule:
- Decoder first attempts enum decoding.
- If that fails, it attempts legacy V1 payload decode.

This allows rolling upgrades without bricking old WAL segments.

### 6.2 Dependency-Ordered ExEx Delivery Algorithm

`depends_on` and `priority` are enforced by deterministic planning:
- Validate all dependencies at registration.
- Reject unknown dependencies.
- Reject duplicate ExEx names.
- Build DAG and perform topological tiering.
- Deliver tier-by-tier; each tier can run in parallel.
- Order within a tier: higher priority first, then name.
- Reject cycles.

### 6.3 Protocol Version Negotiation

Execution must select versioned constants from bundled protocol constants:
- Exact match only.
- Missing patch/minor/major versions fail closed.

This prevents silent semantic mismatches during protocol transitions.

### 6.4 L1 Finality Policy

State roots posted to Ethereum are not considered confirmed until finalized checkpoint criteria are met.

Policy:
- Track the Ethereum block that posted each L2 state root.
- `latest_verified_block` only includes roots whose Ethereum block is finalized.
- Reorg handling drops unfinalized postings from affected Ethereum block onward.

### 6.5 MCP Batch Recursion Guard

`BatchQuery` must be bounded:
- Enforce maximum nesting depth.
- Enforce maximum per-batch query count.
- Reject payloads that exceed limits.

This prevents recursive amplification and memory/CPU blowups.

### 6.6 Performance Validation Methodology

Targets are enforced by repeatable measurement rather than aspiration:
- Fixed benchmark hardware profile and network profile.
- Fixed Starknet block ranges per run.
- Baseline comparison against Juno/Pathfinder for relevant endpoints.
- CI regression gates on p95/p99 latency and sync throughput.
- Publish benchmark metadata with each result (commit hash, dataset range, CPU, RAM, kernel).

## 7. Build Phases

1. Sync-capable node (storage + P2P + checkpoint sync)
2. Canonical validation (BlockifierVM)
3. Wallet-usable node (JSON-RPC)
4. Observability (ExEx manager + OTel)
5. Performance path (Cairo Native integration)
6. Agent interface (MCP server ExEx)
7. BTCFi monitoring (standard wrappers + strkBTC)
8. Optional proving pipeline

## 8. Implementation Status in This Repository

The repository currently includes core and phase-aligned scaffolding crates that codify and test the hardening clauses:
- WAL versioned notification decoding with legacy fallback
- Dependency-aware ExEx delivery planning with cycle/unknown checks
- Protocol constants resolver with exact-match fail-closed semantics (no patch fallback)
- L1 finality gating and unfinalized reorg invalidation
- MCP batch recursion/size validation
- MCP access control hardened with Argon2id API-key derivation (legacy PBKDF2 fallback only on derivation failure)
- MCP server request execution path with authenticated tool handling and deterministic batch response ordering
- Bounded block/state-diff validation (`MAX_TRANSACTIONS_PER_BLOCK`, bounded storage writes/nonces/classes) at ingest boundaries
- In-memory state guardrails against contract/slot/entry explosion with explicit state-limit errors
- Storage backend behavior (sequential inserts, block snapshots, deterministic state roots)
- Dual execution mismatch handling with canonical-state safety
- Blockifier adapter fail-fast guard for account transactions when class-provider integration is unavailable
- ExEx registration hardening: per-ExEx credentials, allowlisted identities, trusted in-process sink enforcement, and sink recovery cooldown/re-enable
- OTel-style ExEx sink implementation with deterministic notification-derived counters and block-regression telemetry
- Node config hardening with validated `ChainId` parsing (mainnet/sepolia/custom strict format)
- JSON-RPC server request handling with strict JSON-RPC envelope validation and deterministic error mapping
- Type-state node builder enforcing storage-before-execution composition
- ExEx manager behavior (registration DAG validation, bounded queue, WAL replay compatibility)

See code in:
- `crates/node-spec-core/src/notification.rs`
- `crates/node-spec-core/src/exex_delivery.rs`
- `crates/node-spec-core/src/protocol_version.rs`
- `crates/node-spec-core/src/l1_finality.rs`
- `crates/node-spec-core/src/mcp.rs`
- `crates/types/src/lib.rs`
- `crates/storage/src/lib.rs`
- `crates/execution/src/lib.rs`
- `crates/mcp-server/src/lib.rs`
- `crates/rpc/src/lib.rs`
- `crates/node/src/lib.rs`
- `crates/exex-manager/src/lib.rs`
- `crates/exex-otel/src/lib.rs`
