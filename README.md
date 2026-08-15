<h1 align="center">AdvisKV</h1>

<p align="center">
  <b>C++17 分布式 KV 存储系统</b><br/>
  库表管理 · 分片路由 · Raft 复制 · 副本数调整 · 故障恢复
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white" alt="C++17" />
  <img src="https://github.com/advisedy/adviskv/actions/workflows/ci.yml/badge.svg" alt="CI" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License" /></a>
  <img src="https://img.shields.io/badge/Recommended-Linux%20%7C%20Ubuntu%2024.04-2ea44a" alt="Recommended environment: Linux (Ubuntu 24.04)" />
  <img src="https://img.shields.io/badge/Status-v0.1.0-blue" alt="Status v0.1.0" />
</p>

<p align="center">
  <a href="README.en.md">English</a> ·
  <a href="#快速开始">快速开始</a> ·
  <a href="#设计博客">设计博客</a> ·
  <a href="docs/v0.1.0/benchmark/README.md">Benchmark</a> ·
  <a href="#当前限制">当前限制</a>
</p>

---

## 项目简介

AdvisKV 是我用 C++17 从零实现的分布式 Key-Value 存储系统。客户端以 `db + table + key` 访问数据：Catalog 管理库表与 DDL，Topo 管理节点、分片副本与路由，Storage 通过 Raft 复制写入，并配合 WAL / Snapshot 做持久化与恢复，SDK 将请求路由到对应分片 Leader。

当前版本为 **v0.1.0**。已支持能力见下一节，边界见文末 [当前限制](#当前限制)；更细的说明与链接见 [版本说明](docs/版本.md)。

## 目前支持

- **库表与分片路由**：支持建库、建表和基本 DDL；SDK 按 `db + table + key` 获取路由，并把请求发送到对应分片的 Leader。
- **副本数在线调整与坏副本替换**：`AlterTableReplicaCount` 支持从 0 个副本启动、缩容到 0 个以及 `N → M`。副本进入 `LOST` 或 `ERROR` 后，Topo 会清理旧副本并补充新副本，Storage 通过 Raft 成员变更将其加入集群。
- **Raft 复制与恢复**：Storage 使用 Raft 复制 KV 写入，并通过 WAL、Snapshot、日志追赶和重启恢复保持副本状态；选举侧实现了 PreVote，降低旧 Leader 网络恢复后干扰新 Leader 的情况。
- **SDK 重试和幂等**：SDK 支持路由刷新和请求重试。Put/Delete 可以带上 `request_id`，相同写重试不会被重复执行；若最终无法确认是否提交则返回 UNKNOWN。Leader 故障切主场景下，写失败比例相关验收见 [SDK 重试验收](docs/v0.1.0/retry.md)。
- **测试与状态观测**：GoogleTest 覆盖 Raft、Replica、WAL、Snapshot 等模块，Python E2E 覆盖多进程链路；服务端和 SDK 提供日志与 metrics。本地 benchmark 见 [v0.1.0 Benchmark](docs/v0.1.0/benchmark/README.md)。

## 快速开始

环境要求：推荐使用 Linux（Ubuntu 24.04）、C++17 编译器、CMake 3.20+、Ninja、Git 和 Python 3。

首次构建前初始化依赖：

```bash
git submodule update --init --recursive
./scripts/setup.sh
./scripts/build.sh
```

如果需要运行 Maelstrom 测试，把上面的 setup 命令替换为：

```bash
./scripts/setup.sh --with-maelstrom-test
```

启动本地集群并打开 `adviskvctl`：

```bash
./scripts/adviskvctl_demo.sh
```

在交互式 shell 中执行：

```text
create_db demo_db dc1
create_table demo_db demo_table 1 1 default
wait_table demo_db demo_table
put demo_db demo_table k1 v1
get demo_db demo_table k1
route demo_db demo_table k1
quit
```

本地演示：

<p align="center">
  <img src="docs/assets/adviskvctl_demo.gif" alt="adviskvctl demo" width="960" />
</p>

Demo 退出时会清理本地进程；也可以手动执行：

```bash
./scripts/stop_cluster.sh
```

### 手动构建

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
```

也可以通过环境变量选择构建类型或目标：

```bash
BUILD_TYPE=Release ./scripts/build.sh
BUILD_TARGETS="catalog topo storage adviskvctl" ./scripts/build.sh
```

主要二进制位于 `build/bin/`。

## 架构

<p align="center">
  <img src="docs/assets/mermaid/minimal-overview.png" alt="AdvisKV architecture overview" width="820" />
</p>

| 模块 | 作用 |
| --- | --- |
| Catalog | 保存库表定义，处理建库、建表和其他 DDL。 |
| Topo | 管理 Storage 节点、分片副本和路由，推进副本状态变化。 |
| Storage | 为分片提供 KV 读写，用 Raft、WAL 和 Snapshot 完成复制与恢复。 |
| SDK | 获取并缓存路由，将请求发送到对应分片的 Storage Leader。 |

一次写请求的大致路径：

```text
SDK → Topo 路由 → Storage Leader → Raft → WAL / KV StateMachine
```

数据面时序：

<p align="center">
  <img src="docs/assets/mermaid/put 链路.png" alt="Put request end-to-end timeline" width="880" />
</p>

模块图：

- [Topo 最小架构](docs/assets/mermaid/topo-minimal-architecture.png)
- [Storage 最小架构](docs/assets/mermaid/storage-minimal-architecture.png)
- [Catalog 最小架构](docs/assets/mermaid/catalog-minimal-architecture.png)

## 测试

运行测试：

```bash
./scripts/run_test.sh
```

如果本地已经安装 Maelstrom，还会先运行一个无故障的 3 节点 Raft 测试，再运行一个 5 节点的故障注入压力测试，否则跳过 Maelstrom。
想要跑 Maelstrom，可以使用
```bash
./scripts/setup.sh --with-maelstrom-test
```
先安装Maelstrom

运行 Python E2E 测试：

```bash
./scripts/e2e_pytest.sh
```

生成覆盖率报告：

```bash
./scripts/coverage.sh
```

当前仓库包含两百多个 GoogleTest 用例。E2E 测试覆盖基础 KV 链路、重启恢复、Raft 选主、日志和 Snapshot 追赶、副本数调整、scale-to-zero 以及故障恢复场景。

## 基准测试

Benchmark 测量的是本地多进程环境中的 `SDK → Topo route → Storage Leader → Raft / WAL / KV` 链路。

测试环境：`Mac15,7`、Apple M3 Pro、12 物理核心 / 12 逻辑 CPU、36 GiB 内存；macOS 15.7.4、arm64。集群包含 1 个 Catalog、1 个 Topo 和 5 个 Storage，进程通过 `127.0.0.1` / `localhost` 通信。

![Mixed benchmark](docs/v0.1.0/benchmark/assets/mixed_read_ratio_qps.svg)

默认场景：`threads=16`、`shard_count=2`、`replica_count=3`、`value_size=128`、`requests=30000`。

| Workload | Scenario | success_qps | avg_us | p95_us | p99_us |
| --- | --- | ---: | ---: | ---: | ---: |
| put | baseline | 9799.99 | 1631.30 | 2577 | 5623 |
| get | baseline | 11059.01 | 1445.76 | 1905 | 2211 |
| mixed | read_ratio=0.80 | 8229.54 | 1942.99 | 3358 | 4474 |

完整报告：

- [Put benchmark](docs/v0.1.0/benchmark/benchmark_put.md)
- [Get benchmark](docs/v0.1.0/benchmark/benchmark_get.md)
- [Mixed benchmark](docs/v0.1.0/benchmark/benchmark_mixed.md)

运行单次 benchmark：

```bash
./scripts/bench.sh --workload=put --threads=4 --requests=10000 --replica_count=3
```

运行 benchmark 并采样 metrics：

```bash
./scripts/bench_metrics.sh --workload=put --threads=4 --requests=10000 --replica_count=3
```

报告默认写入 `build/bench/<run_id>/metrics_report.txt`。

## 设计博客

- [AdvisKV 设计博客专栏](https://www.zhihu.com/column/c_2057637599590797905)

## 文档

- [接口规范](docs/接口规范.md)：Catalog、Topo、Storage 和 SDK 的 RPC 与调用语义。
- [配置](conf/README.md)：`conf/` 与 `build/demo|unit_test|e2e_test|bench` 路径说明。
- [v0.1.0 版本说明](docs/版本.md)：版本能力摘要与后续计划。
- [v0.1.0 Benchmark 说明](docs/v0.1.0/benchmark/README.md)：Benchmark 的运行方式和结果说明。
- [v0.1.0 SDK 重试验收](docs/v0.1.0/retry.md)：写请求重试和 leader 故障切主验收结果。

## 项目结构

```text
conf/       配置文件
proto/      gRPC / Protobuf 定义
scripts/    构建、测试、demo 和 benchmark 脚本
src/        Catalog / Topo / Storage / SDK 与通用模块
tools/      adviskvctl、E2E 客户端、benchmark 客户端、Storage 客户端
test/       GoogleTest 和 Python E2E 测试
docs/       设计文档、版本说明与 benchmark
```

## 当前限制

对应 **v0.1.0**（与 [版本说明](docs/版本.md) 中的后续项一致）：

- `request_id` 暂无 TTL；后续考虑例如保留 30 分钟。
- KV 引擎当前是内存 Map；Storage 已通过 WAL / Snapshot 持久化 KV、Raft 状态与 request record，重启后加载回内存。后续考虑接入更成熟的持久化存储引擎。
- 坏副本替换：新副本升为 voter 后，若原坏副本恢复，当前会把新副本踢掉，行为还可以再优化。
- 暂不支持分片分裂（split）与动态 rebalance；分片数在建表时固定。
- Catalog 与 Topo 目前仍是单进程，尚未做控制面高可用。
