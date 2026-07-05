# AdvisKV V1 Put Benchmark

这份报告记录 AdvisKV V1 在本地环境下的 `put` benchmark，用于观察当前写链路的吞吐和延迟表现，并验证在压测环境下能否稳定完成请求。

## 测试范围

默认场景：

```text
workload      = put
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

本次只测试 `put` 场景。每组实验只改变一个变量：

- Threads：改变 `threads`。
- Shard Count：改变 `shard_count`。
- Replica Count：改变 `replica_count`。
- Value Size：改变 `value_size`。

## 测试环境

- 运行方式：`scripts/bench.sh` 拉起本地集群并运行 `bench_client`。
- 本地集群：`1 meta + 1 sdm + 5 storage`，所有进程都运行在同一台机器上，并通过 `127.0.0.1/localhost` 通信。
- 测试机器：`Mac15,7`，Apple M3 Pro，12 物理核心 / 12 逻辑 CPU，36 GiB 内存。
- 操作系统：macOS 15.7.4 (24G517)，arm64。
- 构建方式：Release
- 最近更新时间：`20260705_203718`。
- 原始结果：[`benchmark_results/put_v1_snapshot.csv`](benchmark_results/put_v1_snapshot.csv)。

## 结果摘要

- `threads` 从 1 增加到 32 时，`success_qps` 从约 1030 提升到约 9742，同时 `avg_us`、`p95_us` 和 `p99_us` 也明显上升。
- `replica_count=1` 的写入吞吐约 15863 QPS，高于 `replica_count=3/5`，符合 Raft 多副本写入需要复制和提交的成本预期。
- `value_size` 从 16 增加到 4096 bytes 时，`success_qps` 从约 8024 下降到约 3707，写入 payload 变大后复制、WAL 和网络序列化成本会更明显。
- `shard_count` 在本地单机多进程环境下没有表现出稳定线性提升，结果会受到 Storage node 数量、leader 分布和单机资源竞争影响。
- 所有测试点 `failure=0`。

## Threads

固定 `shard_count=2`、`replica_count=3`、`value_size=128`、`requests=30000`，调整 `threads`。

![Put threads](assets/put_threads_qps.svg)

| threads | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1029.99 | 970.28 | 945 | 1157 | 1515 | 0 |
| 2 | 2145.12 | 931.73 | 864 | 1283 | 2017 | 0 |
| 4 | 4177.99 | 956.79 | 924 | 1158 | 1561 | 0 |
| 8 | 6900.30 | 1158.58 | 1086 | 1579 | 2475 | 0 |
| 16 | 7960.23 | 2008.92 | 1828 | 3087 | 6192 | 0 |
| 32 | 9742.05 | 3282.29 | 2715 | 6185 | 10143 | 0 |

## Shard Count

固定 `threads=16`、`replica_count=3`、`value_size=128`、`requests=30000`，调整 `shard_count`。

![Put shard count](assets/put_shard_count_qps.svg)

| shard_count | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8916.23 | 1793.37 | 1629 | 2516 | 6504 | 0 |
| 2 | 8104.08 | 1973.18 | 1760 | 3275 | 6012 | 0 |
| 4 | 6038.10 | 2648.78 | 2108 | 6083 | 10366 | 0 |
| 8 | 6766.15 | 2363.32 | 2014 | 4814 | 9733 | 0 |

## Replica Count

固定 `threads=16`、`shard_count=2`、`value_size=128`、`requests=30000`，调整 `replica_count`。

![Put replica count](assets/put_replica_count_qps.svg)

| replica_count | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 15862.52 | 1007.59 | 959 | 1388 | 2163 | 0 |
| 3 | 8469.99 | 1887.74 | 1710 | 2985 | 6119 | 0 |
| 5 | 5297.95 | 3018.88 | 2492 | 6169 | 11124 | 0 |

## Value Size

固定 `threads=16`、`shard_count=2`、`replica_count=3`、`requests=30000`，调整 `value_size`。

![Put value size](assets/put_value_size_qps.svg)

| value_size | success_qps | avg_us | p50_us | p95_us | p99_us | failure |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 8023.63 | 1992.33 | 1843 | 2964 | 5495 | 0 |
| 64 | 6812.90 | 2346.97 | 2106 | 3929 | 7252 | 0 |
| 128 | 6802.80 | 2350.80 | 2105 | 3954 | 7385 | 0 |
| 512 | 6898.95 | 2317.98 | 2071 | 3808 | 7209 | 0 |
| 1024 | 5380.52 | 2972.06 | 2542 | 5785 | 10073 | 0 |
| 4096 | 3707.30 | 4314.25 | 3566 | 9225 | 13215 | 0 |

## 复现方式

```bash
BENCH_LOG_LEVEL=warning \
  ./scripts/bench.sh \
  --workload=put \
  --threads=16 \
  --shard_count=2 \
  --replica_count=3 \
  --value_size=128 \
  --requests=30000 \
  --output_json=build/release/bench_results/put_baseline.json
```
