<p align="center">
  <img src="docs/assets/adviskv.jpeg" alt="AdvisKV banner" width="960" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white" alt="C++17" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License" /></a>
  <img src="https://img.shields.io/badge/Recommended-Linux%20%7C%20Ubuntu%2024.04-2ea44a" alt="Recommended environment: Linux (Ubuntu 24.04)" />
  <img src="https://img.shields.io/badge/Status-v0.1.0-blue" alt="Status v0.1.0" />
</p>

<p align="center">
  <a href="README.md">中文版本</a>
</p>

AdvisKV is a distributed Key-Value storage system built from scratch with **C++17 + gRPC + Protobuf + Raft**. Users access data with a `db + table + key` model: `Catalog` manages databases, tables, and DDL, `Topo` manages Storage nodes, shard replicas, and routes, `Storage` replicates KV writes with Raft and uses WAL / snapshots for persistence and recovery, and the SDK sends requests to the corresponding shard leader.

The current release is **v0.1.0**. It supports the main distributed KV path in a local multi-process environment, including database/table creation, sharding, replica-count changes, abnormal replica replacement, Raft leader election, log replication, snapshot catch-up, and restart recovery. Detailed scope and follow-up work are listed in the [version notes](docs/版本.md).

## Supported in v0.1.0

- **Database/table model, sharding, and routing**: supports database creation, table creation, and basic DDL. The SDK resolves routes with `db + table + key` and sends requests to the corresponding shard leader.
- **Online replica-count changes and abnormal replica replacement**: `AlterTableReplicaCount` supports starting with zero replicas, scaling down to zero, and `N -> M` changes. When a replica enters `LOST` or `ERROR`, Topo cleans it up and adds a replacement; Storage uses Raft membership changes to add the new replica.
- **Raft replication and recovery**: Storage replicates KV writes with Raft. KV state, Raft state, and request records are written to WAL and included in snapshots; after a restart, Storage restores them into memory and continues with subsequent WAL entries. The election path also implements PreVote to reduce interference from an old leader after it reconnects.
- **SDK retries and idempotency**: the SDK supports route refresh and request retries. Put/Delete can carry a `request_id`, so retrying the same write does not execute it twice. If the final commit status cannot be confirmed, the SDK returns `UNKNOWN`. The retry acceptance results for leader failover are in the [SDK retry acceptance report](docs/v0.1.0/retry.md).
- **Testing and observability**: GoogleTest covers Raft, Replica, WAL, and Snapshot modules; Python E2E tests cover the multi-process path; services and the SDK expose logs and metrics. Local benchmark results are in the [v0.1.0 benchmark report](docs/v0.1.0/benchmark/README.md).

## Architecture

```text
Client / CLI / Benchmark
        |
        v
       SDK
        |
        | GetRoute(db, table, key)
        v
       TOPO  <-------------------- Storage NodeAgent HeartBeat
        ^                         |
        |                         | Apply ExpectedReplica
        |                         v
   Catalog  ---- PlaceTable ---- Storage Replica / Raft / WAL / KV
```

Main flows:

```text
DDL:
Client -> Catalog CreateTable -> TOPO PlaceTable -> NodeAgent HeartBeat
       -> Apply ExpectedReplica -> Replica READY -> Route READY -> Catalog NORMAL

Data:
SDK -> TOPO GetRoute -> Storage leader Put/Get/Delete -> Raft -> KV StateMachine
```

## Quick Start

Recommended environment: Linux (Ubuntu 24.04), a C++17 compiler, CMake 3.20+, Ninja, Git, and Python 3.

```bash
git submodule update --init --recursive
./scripts/setup.sh
./scripts/build.sh
./scripts/adviskvctl_demo.sh
```

To prepare the optional Maelstrom test environment, replace the setup command above with:

```bash
./scripts/setup.sh --with-maelstrom-test
```

`adviskvctl_demo.sh` starts a local `topo/catalog/storage` cluster and opens an interactive shell. Example commands:

```text
create_db demo_db dc1
create_table demo_db demo_table 1 1 default
wait_table demo_db demo_table
put demo_db demo_table k1 v1
get demo_db demo_table k1
route demo_db demo_table k1
quit
```

The local demo covers database/table creation, KV reads and writes, and route changes after `demo kill`:

<p align="center">
  <video src="docs/assets/adviskvctl_demo_video.mp4" controls width="720">
    If your browser cannot play the embedded video, open the
    <a href="docs/assets/adviskvctl_demo_video.mp4">adviskvctl demo video</a>.
  </video>
</p>

Stop local processes:

```bash
./scripts/stop_cluster.sh
```

## Build

Recommended script-based build:

```bash
./scripts/build.sh
```

You can also invoke CMake directly:

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
```

Common build variables:

```bash
BUILD_TYPE=Debug ./scripts/build.sh
BUILD_TARGETS="catalog topo storage adviskvctl" ./scripts/build.sh
```

Main binaries are generated under `build/bin/` by default.

## Test

```bash
./scripts/run_test.sh
./scripts/e2e_pytest.sh
./scripts/coverage.sh
```

- `run_test.sh`: builds and runs GoogleTest suites; if Maelstrom is installed, it also runs a normal 3-node Raft test followed by a 5-node fault-injection stress test.
- `e2e_pytest.sh`: starts a local multi-process cluster and verifies the Catalog -> Topo -> Storage -> SDK main path.
- `coverage.sh`: generates a coverage report.

The repository contains more than two hundred GoogleTest cases. E2E tests cover basic KV operations, restart recovery, Raft leader election, log and snapshot catch-up, replica-count changes, scale-to-zero, and failure-recovery scenarios.

## Benchmark

The benchmark reports below measure local flows through `SDK -> Topo route -> Storage leader -> Raft/WAL/KV`. The results come from a local multi-process setup and are intended for tuning and regression comparison.

Test environment: `Mac15,7`, Apple M3 Pro, 12 physical cores / 12 logical CPUs, 36 GiB memory, macOS 15.7.4, arm64. Local cluster: `1 catalog + 1 topo + 5 storage`, all processes running on the same machine and communicating through `127.0.0.1/localhost`.

![Mixed benchmark](docs/v0.1.0/benchmark/assets/mixed_read_ratio_qps.svg)

Default scenario: `threads=16`, `shard_count=2`, `replica_count=3`, `value_size=128`, `requests=30000`.

| Workload | Scenario | success_qps | avg_us | p95_us | p99_us |
|---|---|---:|---:|---:|---:|
| put | baseline | 9799.99 | 1631.30 | 2577 | 5623 |
| get | baseline | 11059.01 | 1445.76 | 1905 | 2211 |
| mixed | read_ratio=0.80 | 8229.54 | 1942.99 | 3358 | 4474 |

Full reports:

- [Put benchmark](docs/v0.1.0/benchmark/benchmark_put.md)
- [Get benchmark](docs/v0.1.0/benchmark/benchmark_get.md)
- [Mixed benchmark](docs/v0.1.0/benchmark/benchmark_mixed.md)

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
[bench] metrics report: build/bench/<run_id>/metrics_report.txt
```

The report lists latency histogram deltas and counter deltas that changed during the run, including `sdk_*` rows from the benchmark client, which makes it useful for local tuning and regression comparison.

## Documentation

- [scripts/README.md](scripts/README.md): common script entry points.
- [conf/README.md](conf/README.md): conf layout and local run directories (`demo` / `unit_test` / `e2e_test` / `bench`).
- [v0.1.0 version notes](docs/版本.md): current features, validation results, and follow-up plans (Chinese).
- [docs/v0.1.0/benchmark/README.md](docs/v0.1.0/benchmark/README.md): v0.1.0 benchmark entry and result summary.
- [docs/v0.1.0/retry.md](docs/v0.1.0/retry.md): SDK retry and leader failover acceptance results.
- [docs/接口规范.md](docs/接口规范.md): RPC and SDK interface semantics (Chinese).
- [Design blog](https://www.zhihu.com/column/c_2057637599590797905): project design notes.

## Project Layout

```text
conf/       local config files
proto/      gRPC / Protobuf definitions
scripts/    build, test, demo and benchmark scripts
src/        Catalog, Topo, Storage, SDK and common modules
tools/      adviskvctl, e2e client, benchmark client, storage client
test/       GoogleTest and Python E2E tests
docs/       design notes, version notes and benchmarks
```

## Current limitations

The following limitations apply to **v0.1.0** and are also listed in the [version notes](docs/版本.md):

- `request_id` has no TTL yet; a retention period such as 30 minutes may be added later.
- The KV engine currently uses an in-memory Map. WAL and snapshots persist KV state, Raft state, and request records, and Storage loads them back into memory during recovery. A more mature persistent storage engine may be added later.
- During abnormal replica replacement, if the old replica recovers after the replacement has become a voter, the current behavior removes the new replica; this policy still needs improvement.
- Shard splitting and dynamic rebalancing are not supported; the shard count is fixed when a table is created.
- Catalog and Topo are still single processes and do not yet provide control-plane high availability.

## License

AdvisKV is licensed under the [MIT License](LICENSE).
