# Boss Demo Runbook (Real Data)

This runbook is for a real, non-simulated boss demo using the `starknet-node` daemon.
It uses live upstream Starknet RPC data and validates the local daemon surface before the meeting.

## 1. Preflight

Required tools:

- `cargo`
- `curl`
- `jq`

Optional:

- `tmux` or two terminal tabs (one for daemon logs, one for API checks)

## 2. Recommended Demo Setup

Set the upstream endpoint (or pass your own):

```bash
export STARKNET_RPC_URL="https://rpc.starknet.lava.build"
```

Run the automated smoke pass:

```bash
./scripts/boss-demo-smoke.sh
```

By default this does a clean startup (removes prior demo checkpoint/journal/snapshot state in `.pastis/demo-daemon`).
Set `PASTIS_DEMO_CLEAN_START=0` if you explicitly want to keep previous local state.

If successful, you should see:

- `demo smoke run: PASS`
- a valid `rpc.chain_id`
- `rpc.block_number` either as a number or `N/A (no committed local block yet)` during warmup
- `readyz_http` equal to `200` (caught up) or `503` (still syncing)

## 3. Keep Daemon Running for Live Demo

```bash
PASTIS_DEMO_KEEP_RUNNING=1 ./scripts/boss-demo-smoke.sh
```

This starts the daemon, validates it, then keeps it running for presentation.

Default bind: `127.0.0.1:9545`

## 4. Live Demo Sequence (8-10 min)

1. Show daemon process and startup config

```bash
ps -p <daemon_pid> -o pid,etime,command
```

2. Show liveness/readiness

```bash
curl -i http://127.0.0.1:9545/healthz
curl -i http://127.0.0.1:9545/readyz
```

3. Show sync/runtime status

```bash
curl -s http://127.0.0.1:9545/status -H "authorization: Bearer pastis-demo-token" | jq
```

4. Show core JSON-RPC calls

```bash
curl -s http://127.0.0.1:9545/ \
  -H "authorization: Bearer pastis-demo-token" \
  -H "content-type: application/json" \
  --data '{"jsonrpc":"2.0","id":1,"method":"starknet_chainId","params":[]}' | jq

curl -s http://127.0.0.1:9545/ \
  -H "authorization: Bearer pastis-demo-token" \
  -H "content-type: application/json" \
  --data '{"jsonrpc":"2.0","id":2,"method":"starknet_blockNumber","params":[]}' | jq

curl -s http://127.0.0.1:9545/ \
  -H "authorization: Bearer pastis-demo-token" \
  -H "content-type: application/json" \
  --data '{"jsonrpc":"2.0","id":3,"method":"starknet_getBlockWithTxs","params":["latest"]}' | jq
```

5. Show ops counters

```bash
curl -s http://127.0.0.1:9545/metrics -H "authorization: Bearer pastis-demo-token" | head -n 40
```

## 5. Fallback Plan (If Upstream Is Slow)

If upstream latency spikes during the meeting:

1. keep daemon running
2. use `/status`, `/metrics`, and last successful JSON-RPC outputs from your rehearsal logs
3. continue narrative with observed live sync progression and health checks

## 6. Rehearsal Acceptance Criteria

Demo is considered ready when all are true:

- smoke script passes end-to-end
- `/healthz` is reachable (`200` healthy or `503` degraded-but-serving)
- authenticated `/status` returns valid JSON
- `starknet_chainId` and `starknet_syncing` succeed
- if a local block is committed during warmup, `starknet_blockNumber` and `starknet_getBlockWithTxs` succeed
- `/metrics` includes `pastis_rpc_requests_total`
