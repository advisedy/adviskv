# AdvisKV

**C++17 distributed KV storage system**

Database and table management · Hash-range sharding · Raft replication · Replica-count changes · Persistence and failure recovery

![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white) ![CI](https://github.com/advisedy/adviskv/actions/workflows/ci.yml/badge.svg) ![MIT License](https://img.shields.io/badge/License-MIT-green.svg) ![Recommended environment: Linux (Ubuntu 24.04)](https://img.shields.io/badge/Recommended-Linux%20%7C%20Ubuntu%2024.04-2ea44a) ![Status v0.1.0](https://img.shields.io/badge/Status-v0.1.0-blue)

[中文](README.md) · [Quick Start](#quick-start) · [Design Blog](#design-blog) · [Benchmark](#benchmark) · [Current Limitations](#current-limitations)

---

## Overview

AdvisKV is a small but complete distributed Key-Value storage system written in C++17. The current version is v0.2.0.

Data is organized as `db + table + key`. Meta manages databases, tables, and the cluster; Storage stores data; clients access it through the SDK.

## Currently Supported

- Put, Get, Delete, and compare-and-swap (CAS), which updates a value only when it matches the expected value.
- Database and table management, hash-range sharding, and SDK route caching.
- Online replica-count changes and automatic replacement of failed replicas.
- Raft replication, ReadIndex-based linearizable reads, and PreVote elections.
- Write-ahead log (WAL) and snapshot persistence, with Storage and Meta restart recovery.
- Automatic SDK retries and write-request deduplication.
- Unit tests, multi-process end-to-end (E2E) tests, logs, and metrics.

## Quick Start

Recommended environment: Linux (Ubuntu 24.04), a C++17 compiler, CMake 3.20+, Ninja, pkg-config, Git, and Python 3.

Initialize dependencies before the first build:

```bash
git submodule update --init --recursive
./scripts/setup.sh
./scripts/build.sh
```

To run Maelstrom tests, replace the setup command above with:

```bash
./scripts/setup.sh --with-maelstrom-test
```

Start a local cluster and open the `adviskvctl` shell:

```bash
./scripts/adviskvctl_demo.sh
```

Run these commands in the interactive shell:

```text
create_db demo_db dc1
create_table demo_db demo_table 1 1 default
wait_table demo_db demo_table
put demo_db demo_table k1 v1
get demo_db demo_table k1
cas demo_db demo_table k1 v1 v2
get demo_db demo_table k1
shards demo_db demo_table
route demo_db demo_table k1
quit
```

Local demo:

![adviskvctl demo](docs/assets/adviskvctl_demo.gif)

The demo stops its cluster on exit. You can also stop it manually:

```bash
./scripts/stop_cluster.sh
```

### Manual Build

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
```

You can also select the build type or targets through environment variables:

```bash
BUILD_TYPE=Release ./scripts/build.sh
BUILD_TARGETS="meta storage adviskvctl" ./scripts/build.sh
```

Main binaries are generated under `build/bin/`.

## Architecture

![AdvisKV architecture overview](docs/assets/mermaid/minimal-overview.png)

| Module | Responsibility |
| --- | --- |
| Meta | Persists database and table definitions, and manages Storage nodes, replicas, and routes. |
| Storage | Serves shard KV requests and uses Raft, WAL, and snapshots for replication, persistence, and recovery. |
| SDK | Caches routes and sends requests to the corresponding leader. |

A write follows this path:

```text
SDK → RouteCache → Storage Leader
    → Raft → WAL persistence / quorum commit → KV StateMachine
```

//todo

## Tests

Run C++ tests:

```bash
./scripts/run_test.sh
```

The script builds and runs GoogleTest first. If Maelstrom and its test adapter are available, it also runs a fault-free 3-node Raft test followed by a 5-node network-partition stress test. Maelstrom tests are skipped when it is not installed. Use `./scripts/setup.sh --with-maelstrom-test` to prepare the optional environment.

Run Python E2E tests:

```bash
./scripts/e2e_pytest.sh
```

Generate a coverage report:

```bash
./scripts/coverage.sh
```

## Benchmark

[v0.1.0 historical benchmark report](docs/v0.1.0/benchmark/README.md)

## Design Blog

- [AdvisKV design blog series](https://www.zhihu.com/column/c_2057637599590797905) (Chinese)

## Documentation

- [Interface specification](docs/接口规范.md): remote procedure call (RPC) and SDK semantics for Meta and Storage; see `proto/` and `src/sdk/client.h` for the latest definitions (Chinese).
- [Configuration](conf/README.md): configuration files under `conf/` and run directories under `build/demo|unit_test|e2e_test|bench`.
- [v0.1.0 version notes](docs/版本.md): feature summary and follow-up plans (Chinese).
- [v0.1.0 historical benchmark](docs/v0.1.0/benchmark/README.md): original test environment and results.
- [v0.1.0 SDK retry acceptance report](docs/v0.1.0/retry.md): historical write-retry and leader-failover test results.

## Project Layout

```text
conf/       configuration files
proto/      gRPC / Protobuf definitions
scripts/    build, test, demo and benchmark scripts
src/        Meta / Storage / SDK and common modules
tools/      adviskvctl, E2E client, benchmark client, Storage client
test/       GoogleTest and Python E2E tests
docs/       design documents, version notes and benchmarks
```

## Current Limitations

The following limitations describe the current implementation:

- `request_id` records have no time-to-live (TTL) yet.
- The KV engine currently uses an in-memory Map.
- During failed-replica replacement, recovery of the original replica may cause the new replica to be removed first.
- Shard splitting, dynamic replica migration, and automatic rebalancing are not supported yet.
- Meta still runs as a single process and does not provide control-plane high availability.
