# AdvisKV V1 Get Benchmark

这份报告记录 AdvisKV V1 在本地环境下的 `get` benchmark，用于观察当前读链路的吞吐和延迟表现，并验证在压测环境下能否稳定完成请求。

## 测试范围

默认场景：

```text
workload      = get
threads       = 16
shard_count   = 2
replica_count = 3
value_size    = 128
requests      = 30000
```

除上面这些参数外，其余参数使用 `bench_client` 默认值：

```text
key_count       = 1000
warmup_requests = 0
```

本次只测试 `get` 场景。每组实验只改变一个变量：

- Threads：改变 `threads`。
- Shard Count：改变 `shard_count`。
- Replica Count：改变 `replica_count`。
- Value Size：改变 `value_size`。

当前 SDK 的 `get` 会通过 SDM 下发的 route 选择 leader replica 读，并经过 Storage 侧 ReadIndex 检查。因此这里的 get benchmark 衡量的是 leader-based linearizable read path，不是 follower read 或多副本分摊读流量。

## 测试环境

- 运行方式：`scripts/bench.sh` 拉起本地集群并运行 `bench_client`。
- 本地集群：`1 meta + 1 sdm + 5 storage`，所有进程都运行在同一台机器上，并通过 `127.0.0.1/localhost` 通信。
- 每个测试点会先写入 `key_count=1000` 的可读数据集，这部分准备阶段不计入正式 benchmark 结果。
- 测试机器：`Mac15,7`，Apple M3 Pro，12 物理核心 / 12 逻辑 CPU，36 GiB 内存。
- 操作系统：macOS 15.7.4 (24G517)，arm64。
- 构建方式：Release
- 生成时间：`20260705_205947`。
- 原始结果：[`benchmark_results/get_v1_snapshot.csv`](benchmark_results/get_v1_snapshot.csv)。

## 结果摘要

- `threads` 从 1 增加到 32 时，`success_qps` 从约 1949 提升到约 10015，同时 `avg_us`、`p95_us` 和 `p99_us` 也随并发上升而增加。
- `replica_count=1` 的读吞吐约 21733 QPS，`replica_count=3/5` 分别约 9271/6026 QPS。当前读路径仍走 leader 和 ReadIndex，因此副本数不代表读流量会被 follower 自动分摊。
- `value_size` 从 16 增加到 4096 bytes 时，`success_qps` 从约 8573 变化到约 8570。
- `shard_count` 在本地单机多进程环境下不一定稳定线性提升，结果会受到 Storage node 数量、leader 分布和单机资源竞争影响。
- 所有测试点 `failure=0`。

## Threads

固定 `shard_count=2`、`replica_count=3`、`value_size=128`、`requests=30000`，调整 `threads`。

![Get threads](assets/get_threads_qps.svg)

| threads | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1948.71 | 512.60 | 511 | 552 | 588 | 0 |
| 2 | 3631.44 | 550.18 | 552 | 604 | 653 | 0 |
| 4 | 6099.37 | 655.23 | 635 | 818 | 1009 | 0 |
| 8 | 7124.22 | 1122.15 | 1107 | 1385 | 1557 | 0 |
| 16 | 8879.04 | 1800.89 | 1752 | 2375 | 2831 | 0 |
| 32 | 10014.74 | 3193.46 | 3090 | 4504 | 5364 | 0 |

## Shard Count

固定 `threads=16`、`replica_count=3`、`value_size=128`、`requests=30000`，调整 `shard_count`。

![Get shard count](assets/get_shard_count_qps.svg)

| shard_count | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9846.85 | 1623.87 | 1585 | 2018 | 2287 | 0 |
| 2 | 8947.03 | 1787.30 | 1737 | 2440 | 2957 | 0 |
| 4 | 9264.39 | 1726.00 | 1683 | 2336 | 3013 | 0 |
| 8 | 9108.16 | 1755.57 | 1697 | 2392 | 3268 | 0 |

## Replica Count

固定 `threads=16`、`shard_count=2`、`value_size=128`、`requests=30000`，调整 `replica_count`。

![Get replica count](assets/get_replica_count_qps.svg)

| replica_count | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 21732.71 | 735.20 | 714 | 985 | 1219 | 0 |
| 3 | 9270.94 | 1724.79 | 1686 | 2193 | 2560 | 0 |
| 5 | 6026.20 | 2653.99 | 2612 | 3282 | 3857 | 0 |

## Value Size

固定 `threads=16`、`shard_count=2`、`replica_count=3`、`requests=30000`，调整 `value_size`。

![Get value size](assets/get_value_size_qps.svg)

| value_size | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 8573.46 | 1865.12 | 1803 | 2439 | 3254 | 0 |
| 64 | 8612.90 | 1856.60 | 1804 | 2469 | 3040 | 0 |
| 128 | 8669.53 | 1844.45 | 1790 | 2410 | 3082 | 0 |
| 512 | 8555.62 | 1869.03 | 1805 | 2507 | 3271 | 0 |
| 1024 | 8647.08 | 1849.20 | 1798 | 2488 | 2926 | 0 |
| 4096 | 8569.62 | 1865.95 | 1815 | 2563 | 3111 | 0 |

## 复现方式


```bash
BENCH_LOG_LEVEL=warning \
  ./scripts/bench.sh \
  --workload=get \
  --threads=16 \
  --shard_count=2 \
  --replica_count=3 \
  --value_size=128 \
  --requests=30000 \
  --output_json=build/release/bench_results/get_baseline.json
```
