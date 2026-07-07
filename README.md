<p align="center">
  <img src="docs/assets/adviskv.jpeg" alt="AdvisKV banner" width="960" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white" alt="C++17" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License" /></a>
  <img src="https://img.shields.io/badge/Platform-macOS%20%7C%20Linux-2ea44f" alt="Platform macOS and Linux" />
  <img src="https://img.shields.io/badge/Status-V1%20prototype-blue" alt="V1 prototype" />
</p>

<p align="center">
  <a href="README.zh-CN.md">中文版本</a>
</p>

AdvisKV is a distributed Key-Value storage prototype implemented with **C++17 + gRPC + Protobuf + Raft**. It uses a control-plane/data-plane architecture: `Meta` manages DDL and catalog metadata, `SDM` manages Storage node registration, heartbeats, replica-group orchestration, desired replica dispatch, and shard route convergence, `Storage` handles KV reads/writes, Raft replication, WAL, snapshots, and local recovery, and `SDK` exposes `put/get/delete` APIs based on `db + table + key`.

The current V1 runs a complete local main path: database/table creation, table sharding, multi-replica placement, replica-count changes, abnormal replica replacement, route generation, SDK reads/writes, Raft replication, WAL, snapshots, crash recovery, GoogleTest, Python E2E tests, benchmarks, and metrics text reports. It is designed as a runnable prototype for learning distributed KV architecture and local experimentation, not as a production-ready database.

## Features

- Database/table DDL: create/drop database, create/drop table, and `AlterTableReplicaCount`.
- Sharded table model: each table is split into shards, and each shard can have multiple Storage replicas.
- SDM-driven desired-state orchestration: Storage NodeAgent heartbeats report observed state, and SDM returns expected replica actions.
- Replica-count scaling flows, including `0 -> N`, `N -> 0`, and multi-replica resize scenarios.
- Abnormal replica replacement: SDM adds new replicas when healthy members are insufficient and cleans up `LOST` / `ERROR` members.
- SDK route resolution based on `db + table + key`, followed by leader-based `put/get/delete` requests to Storage.
- Storage replication based on Raft, with WAL, snapshots, and crash recovery.
- GoogleTest, Python E2E tests, benchmark tooling, and service/SDK metrics reports without a Prometheus dependency.

## Architecture

```text
Client / CLI / Benchmark
        |
        v
       SDK
        |
        | GetRoute(db, table, key)
        v
       SDM  <-------------------- Storage NodeAgent HeartBeat
        ^                         |
        |                         | Apply ExpectedReplica
        |                         v
      Meta  ---- PlaceTable ---- Storage Replica / Raft / WAL / KV
```

Main flows:

```text
DDL:
Client -> Meta CreateTable -> SDM PlaceTable -> NodeAgent HeartBeat
       -> Apply ExpectedReplica -> Replica READY -> Route READY -> Meta NORMAL

Data:
SDK -> SDM GetRoute -> Storage leader Put/Get/Delete -> Raft -> KV StateMachine
```

## Quick Start

Requirements: macOS or Linux, a C++17 compiler, CMake 3.20+, Ninja, Git, and Python 3.

```bash
./scripts/setup.sh
./scripts/build.sh
./scripts/adviskvctl_demo.sh
```

`adviskvctl_demo.sh` starts a local `sdm/meta/storage` cluster and opens an interactive shell. Example commands:

```text
create_db demo_db dc1
create_table demo_db demo_table 1 1 default
wait_table demo_db demo_table
put demo_db demo_table k1 v1
get demo_db demo_table k1
route demo_db demo_table k1
quit
```

Stop local processes:

```bash
./scripts/stop_cluster.sh
```

## Build

Recommended script-based build:

```bash
./scripts/build.sh
```

You can also use CMake presets:

```bash
cmake --preset release
cmake --build --preset release
```

Common build variables:

```bash
BUILD_TYPE=Debug ./scripts/build.sh
BUILD_TARGETS="meta sdm storage adviskvctl" ./scripts/build.sh
```

Main binaries are generated under `build/release/bin/` by default.

## Test

```bash
./scripts/run_test.sh
./scripts/e2e_pytest.sh
./scripts/coverage.sh
```

- `run_test.sh`: builds and runs GoogleTest suites.
- `e2e_pytest.sh`: starts a local multi-process cluster and verifies the Meta -> SDM -> Storage -> SDK main path.
- `coverage.sh`: generates a coverage report.

E2E tests cover basic KV operations, restart recovery, leader failover, follower catch-up, crash recovery, scale-to-zero, replica-count changes, and failure scenarios.

## Benchmark

The benchmark reports below measure local end-to-end flows through `SDK -> SDM route -> Storage leader -> Raft/WAL/KV`. Results come from one local V1 test environment and should be treated as local regression and tuning references, not production performance claims.

Test environment: `Mac15,7`, Apple M3 Pro, 12 physical cores / 12 logical CPUs, 36 GiB memory, macOS 15.7.4, arm64. Local cluster: `1 meta + 1 sdm + 5 storage`, all processes running on the same machine and communicating through `127.0.0.1/localhost`.

![Mixed benchmark](docs/benchmark/assets/mixed_read_ratio_qps.svg)

Default scenario: `threads=16`, `shard_count=2`, `replica_count=3`, `value_size=128`, `requests=30000`.

| Workload | Scenario | success_qps | avg_us | p95_us | p99_us |
|---|---|---:|---:|---:|---:|
| put | baseline | 9799.99 | 1631.30 | 2577 | 5623 |
| get | baseline | 11059.01 | 1445.76 | 1905 | 2211 |
| mixed | read_ratio=0.80 | 8229.54 | 1942.99 | 3358 | 4474 |

Full reports:

- [Put benchmark](docs/benchmark/benchmark_put.md)
- [Get benchmark](docs/benchmark/benchmark_get.md)
- [Mixed benchmark](docs/benchmark/benchmark_mixed.md)

Run a single benchmark:

```bash
./scripts/bench.sh --workload=put --threads=4 --requests=10000 --replica_count=3
```

Run a benchmark and generate a metrics text report:

```bash
./scripts/bench_metrics.sh --workload=put --threads=4 --requests=10000 --replica_count=3
```

`bench_metrics.sh` uses the same arguments as `bench.sh`, samples service `/metrics` endpoints plus the benchmark client's SDK metrics during the run, and prints the report path at the end:

```text
[bench] metrics report: build/release/bench_results/<run_id>/metrics_report.txt
```

The report lists latency histogram deltas and counter deltas that changed during the run, including `sdk_*` rows from the benchmark client, which makes it useful for local tuning and regression comparison.

## Documentation

- [scripts/README.md](scripts/README.md): common script entry points.
- [docs/benchmark/README.md](docs/benchmark/README.md): V1 benchmark entry and result summary.
- [docs/约束.md](docs/约束.md): V1 API, state, and scale-to-zero semantics.
- [docs/sdm_reconcile_layering.md](docs/sdm_reconcile_layering.md): SDM reconcile layering design.
- [docs/sdm_membership_change_design.md](docs/sdm_membership_change_design.md): replica membership change design.
- [docs/replica_group_service_source_guide.md](docs/replica_group_service_source_guide.md): ReplicaGroupService source guide.

## Project Layout

```text
conf/       local config files
proto/      gRPC / Protobuf definitions
scripts/    build, test, demo and benchmark scripts
src/        Meta, SDM, Storage, SDK and common modules
tools/      adviskvctl, e2e client, benchmark client, storage client
test/       GoogleTest and Python E2E tests
docs/       design notes and V1 constraints
```

## Status

AdvisKV is currently a V1 prototype. It provides a buildable, runnable, and testable distributed KV main path, but the following boundaries should be kept in mind:

- Meta and SDM currently run as single local processes and are not highly available.
- The main path for replica-count changes and abnormal replica replacement is implemented, but more complete automatic rebalancing, complex membership strategies, and production-grade operations are still out of scope for V1.
- Route and writable-leader semantics still need broader failure-scenario validation.
- Storage currently uses a map-based KV engine and has not integrated production-grade LSM engines such as RocksDB.
- Storage and SDK benchmark metrics can be sampled, while Meta does not yet expose the same level of metrics or health endpoints.
- Benchmark results come from local V1 tests and are intended for tuning and regression comparison, not production performance claims.

## License

AdvisKV is licensed under the [MIT License](LICENSE).
