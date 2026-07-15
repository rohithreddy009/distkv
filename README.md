# DistKV

A fault-tolerant distributed key-value store in Go. Raft consensus implemented
from scratch (no consensus libraries), a custom LSM-style storage engine, and
gRPC/Protocol Buffers for both client and inter-node communication.

```
Client ──gRPC──▶ KV Server ──propose──▶ Raft Core ◀──gRPC──▶ Peer nodes
                     ▲                     │
                     │  apply committed    ▼
                State machine ◀──── Raft log (WAL, fsync group commit)
                     │
                     ▼
             Storage engine (memtable + SSTables + compaction)
```

## What's implemented

- **Raft consensus, from scratch** (`raft/`): leader election, log
  replication, persistence, leader no-op commit barrier, fast log
  backtracking on conflicts (conflict term/index hints), snapshots and
  InstallSnapshot for lagging followers, group-commit fsync batching.
- **Storage engine** (`storage/`): write-ahead log with CRC32-checksummed
  records and torn-write recovery, skiplist memtable, immutable SSTables
  with sparse indexes, full merge compaction with tombstone elimination.
- **gRPC layer** (`server/`, `proto/`): separate client-facing and Raft
  services, protobuf schemas, linearizable writes and ReadIndex-validated
  leader reads, exactly-once semantics via client ID + sequence dedup.
- **Client** (`client/`): leader discovery, automatic failover and retry
  with backoff.
- **Fault-injection harness** (`harness/`): in-process cluster with a
  simulated network supporting partitions, per-link cuts, random message
  drops, and latency injection.
- **Observability** (`server/metrics.go`, `monitoring/`): Prometheus
  `/metrics` on each node (ports 9001–9003) and a Grafana dashboard for
  leader status, latency, apply lag, and election events.

## Quick start

Requires Go 1.22+ (protoc + plugins only needed to regenerate protos).

```bash
./scripts/start-cluster.sh     # builds and starts a local 3-node cluster

./bin/distkv-cli put greeting "hello raft"
./bin/distkv-cli get greeting
./bin/distkv-cli status

./scripts/chaos.sh             # kill -9 the leader, measure recovery, verify no data loss
./scripts/stop-cluster.sh
```

## Monitoring (Prometheus + Grafana)

**Option A — local cluster + Docker monitoring**

```bash
./scripts/start-cluster.sh      # nodes expose /metrics on :9001-:9003
./scripts/start-monitoring.sh   # Prometheus :9090, Grafana :3000 (admin/admin)
# open Grafana -> DistKV -> DistKV Overview
./scripts/stop-monitoring.sh
```

**Option B — full stack in Docker**

```bash
docker compose up --build
# KV: localhost:7001-7003  |  Grafana: localhost:3000  |  Prometheus: localhost:9090
```

Example metrics: `distkv_is_leader`, `distkv_kv_request_duration_seconds`,
`distkv_apply_lag`, `distkv_leader_elections_total`.

## Testing

```bash
go test -race ./...
```

The suite covers, among others:

| Area | Scenarios |
|---|---|
| Storage | crash recovery from WAL, torn-write tail, compaction, snapshot/reset |
| Elections | failover, split-brain prevention without quorum, leader rejoin, 20% message loss |
| Replication | follower catch-up, no commit without quorum, divergent log repair, full-cluster crash/restart, 100 concurrent proposals |
| Snapshots | log compaction, InstallSnapshot on lagging follower, restart from snapshot |
| Chaos | randomized rounds of partitions + crashes + flaky network on a 5-node cluster, then verify every acknowledged write is applied everywhere with no divergence |
| End-to-end | real gRPC 3-node cluster: put/get/delete, 200-key workload, leader kill failover |

## Measured results

Local 3-node cluster on one machine (Apple Silicon, macOS), 128-byte values.
Reproduce with `./scripts/start-cluster.sh && go run ./cmd/bench ...` and
`./scripts/chaos.sh`.

| Metric | Result |
|---|---|
| Read throughput (32 readers) | ~76,000 ops/s |
| Read latency | p50 0.32ms, p99 2.1ms |
| Write throughput (64 writers) | ~1,100 ops/s |
| Write latency | p50 55ms, p99 105ms |
| Recovery after `kill -9` of leader | 0.17-0.44s until writes accepted again, no data loss |

Honest notes on these numbers:

- Writes are fsync-bound by design: an acknowledged write is durable on a
  majority (leader + follower fsync before ack). Group commit batches
  concurrent proposals into single fsyncs, which is where write throughput
  scales with concurrency (~250 ops/s at 16 writers vs ~1,100 at 64).
- Reads use **ReadIndex**: quorum confirms leadership, then the state
  machine applies through the read index before lookup (no log write).
- All three nodes share one disk here; a real deployment would show higher
  write throughput per node.

## Design decisions and trade-offs

- **Durability over latency.** The Raft log fsyncs before acknowledging;
  the state-machine engine runs with `SyncWAL=false` since Raft's log
  already guarantees recoverability of unapplied entries.
- **Group commit.** Appends are buffered and one fsync covers every entry
  appended since the last sync. Correctness constraint: the leader only
  counts itself toward a commit majority after its log is synced, and
  followers sync before replying success.
- **Linearizable reads via ReadIndex** — quorum heartbeat proves leadership
  before serving; lease-based reads are a possible next step.
- **Leader no-op entry on election** so a new leader can commit entries
  from prior terms (Raft §5.4.2) instead of stalling.
- **Snapshots**: the state machine serializes its full state every 10,000
  applied entries; the Raft log is then compacted and the WAL rewritten via
  atomic rename. Followers too far behind receive the snapshot over
  InstallSnapshot.
- **Exactly-once writes**: clients tag requests with an ID and sequence
  number; the state machine dedupes retried commands (retries happen
  naturally on leader failover).

## Layout

```
proto/     protobuf definitions (raft + kv services)
raft/      Raft core: node state machine, disk persistence
storage/   LSM engine: WAL, memtable, SSTables, compaction
server/    gRPC services, state machine, ReadIndex reads
client/    client library with leader discovery/retry
harness/   in-process cluster + simulated network for fault injection
cmd/       distkv (node), distkv-cli (client), bench (load generator)
scripts/   cluster launcher, chaos test
```

## Limitations / future work

- Static membership (no joint-consensus configuration changes).
- Single Raft group; consistent-hash sharding across groups is future work.
- Lease-based reads not yet implemented (each read does a quorum heartbeat).
- Snapshot install streams the whole snapshot in one RPC message.
