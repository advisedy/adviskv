# AdvisKV

<p align="center">
  <img src="docs/assets/adviskv.jpeg" alt="AdvisKV banner" width="960" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white" alt="C++17" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License" /></a>
  <img src="https://img.shields.io/badge/Platform-macOS%20%7C%20Linux-2ea44f" alt="Platform macOS and Linux" />
  <img src="https://img.shields.io/badge/Status-V1%20prototype-blue" alt="V1 prototype" />
</p>

AdvisKV 是一个使用 **C++17 + gRPC + Protobuf + Raft** 实现的分布式 Key-Value 存储原型。项目采用控制面和数据面拆分架构：`Meta` 负责 DDL 和 catalog，`SDM` 负责节点、表、副本和 route 收敛，`Storage` 负责 KV 读写、Raft 复制、WAL、snapshot 和本地恢复，`SDK` 对外提供基于 `db + table + key` 的访问接口。

当前 V1 已经跑通一条完整主链路：建库建表、表分片、多副本、调整副本数、异常副本替换、route 生成、SDK 读写、Raft 复制、WAL、snapshot、crash recovery、gtest、E2E、benchmark 和 Storage metrics 文本报告。它更适合作为分布式 KV 架构和本地实验工具链的可运行原型，而不是可直接生产部署的数据库。

## Features

- 建库、删库、建表、删表和 `AlterTableReplicaCount`。
- 表按 shard 拆分，每个 shard 可以部署多个 Storage replica。
- SDM 根据 NodeAgent 心跳下发期望副本，并维护 replica、replica group 和 route 状态。
- 支持 `0 -> N`、`N -> 0` 和多副本扩缩容场景。
- 支持异常副本 replacement：健康副本不足时补充新副本，并清理 `LOST` / `ERROR` 成员。
- SDK 根据 `db + table + key` 自动解析 route，并访问 Storage leader 执行 `put/get/delete`。
- Storage 基于 Raft 做副本复制，配套 WAL、snapshot 和 crash recovery。
- 提供 gtest、Python E2E、benchmark 和无 Prometheus 依赖的 metrics 文本报告。

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

核心链路：

```text
DDL:
Client -> Meta CreateTable -> SDM PlaceTable -> NodeAgent HeartBeat
       -> Apply ExpectedReplica -> Replica READY -> Route READY -> Meta NORMAL

Data:
SDK -> SDM GetRoute -> Storage leader Put/Get/Delete -> Raft -> KV StateMachine
```

## Quick Start

准备环境：macOS 或 Linux、C++17 编译器、CMake 3.20+、Ninja、Git、Python 3。

```bash
./scripts/setup.sh
./scripts/build.sh
./scripts/adviskvctl_demo.sh
```

`adviskvctl_demo.sh` 会自动拉起本地 `sdm/meta/storage`，并进入交互式 shell。可以输入：

```text
create_db demo_db dc1
create_table demo_db demo_table 1 1 default
wait_table demo_db demo_table
put demo_db demo_table k1 v1
get demo_db demo_table k1
route demo_db demo_table k1
quit
```

停止本地进程：

```bash
./scripts/stop_cluster.sh
```

## Build

推荐使用脚本构建：

```bash
./scripts/build.sh
```

也可以使用 CMake presets：

```bash
cmake --preset release
cmake --build --preset release
```

常用构建变量：

```bash
BUILD_TYPE=Debug ./scripts/build.sh
BUILD_TARGETS="meta sdm storage adviskvctl" ./scripts/build.sh
```

主要二进制默认位于 `build/release/bin/`。

## Test

```bash
./scripts/run_test.sh
./scripts/e2e_pytest.sh
./scripts/coverage.sh
```

- `run_test.sh`：构建并运行 GoogleTest。
- `e2e_pytest.sh`：拉起本地多进程集群，验证 Meta -> SDM -> Storage -> SDK 主链路。
- `coverage.sh`：生成覆盖率报告。

E2E 覆盖基础 KV、重启恢复、leader failover、follower catch-up、crash recovery、scale-to-zero、副本数调整和故障场景。

## Benchmark

AdvisKV 提供本地端到端 benchmark，用于观察 V1 prototype 在 `SDK -> SDM route -> Storage leader -> Raft/WAL/KV` 主链路上的表现。以下结果是 local V1 snapshot，不是生产性能承诺。

测试环境：`Mac15,7`，Apple M3 Pro，12 物理核心 / 12 逻辑 CPU，36 GiB 内存；macOS 15.7.4，arm64。本地集群：`1 meta + 1 sdm + 5 storage`，所有进程都运行在同一台机器上，并通过 `127.0.0.1/localhost` 通信。

![Mixed benchmark](docs/benchmark/assets/mixed_read_ratio_qps.svg)

默认场景：`threads=16`、`shard_count=2`、`replica_count=3`、`value_size=128`、`requests=30000`。

| Workload | Scenario | success_qps | avg_us | p95_us | p99_us |
|---|---|---:|---:|---:|---:|
| put | baseline | 7960.23 | 2008.92 | 3087 | 6192 |
| get | baseline | 8879.04 | 1800.89 | 2375 | 2831 |
| mixed | read_ratio=0.80 | 7537.85 | 2121.47 | 3507 | 4686 |

完整 benchmark 报告：

- [Put benchmark](docs/benchmark/benchmark_put.md)
- [Get benchmark](docs/benchmark/benchmark_get.md)
- [Mixed benchmark](docs/benchmark/benchmark_mixed.md)

运行单次 benchmark：

```bash
./scripts/bench.sh --workload=put --threads=4 --requests=10000 --replica_count=3
```

运行 benchmark 并生成 Storage metrics 文本报告：

```bash
./scripts/bench_metrics.sh --workload=put --threads=4 --requests=10000 --replica_count=3
```

`bench_metrics.sh` 与 `bench.sh` 使用同一套参数，只是在 benchmark 期间采样 Storage `/metrics`，结束后输出报告路径：

```text
[bench] metrics report: build/release/bench_results/<run_id>/metrics_report.txt
```

报告会列出本次 benchmark 期间有增量的 Storage latency histogram 和 counter delta，适合本地调优和回归比较。

## Documentation

- [scripts/README.md](scripts/README.md)：常用脚本入口说明。
- [docs/benchmark/README.md](docs/benchmark/README.md)：V1 benchmark 入口和结果摘要。
- [docs/约束.md](docs/约束.md)：V1 API、状态和 scale-to-zero 语义。
- [docs/sdm_reconcile_layering.md](docs/sdm_reconcile_layering.md)：SDM reconcile 分层设计。
- [docs/sdm_membership_change_design.md](docs/sdm_membership_change_design.md)：副本成员变更设计。
- [docs/replica_group_service_source_guide.md](docs/replica_group_service_source_guide.md)：ReplicaGroupService 源码导读。

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

AdvisKV 当前是 V1 prototype。已经具备可构建、可运行、可测试的分布式 KV 主链路，但仍需谨慎看待这些边界：

- Meta 和 SDM 目前是单进程本地模式，不是高可用部署。
- 已有副本数调整和异常副本 replacement 主链路，但更完整的自动 rebalance、复杂成员变更策略和生产级运维策略仍在演进。
- route 和 leader 可写路由语义还需要更多故障场景验证。
- Storage 当前使用 map-based KV engine，还未集成 RocksDB 等生产级 LSM 引擎。
- Storage metrics 已可采样，Meta/SDM 还没有同等级别的 metrics/health 暴露。
- Benchmark 已沉淀为 local V1 snapshot，用于本地调优和回归比较，不代表生产性能承诺。

## License

AdvisKV is licensed under the [MIT License](LICENSE).
