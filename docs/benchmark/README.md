# AdvisKV Benchmark

AdvisKV benchmark 记录本地 V1 的端到端 KV 表现，链路覆盖 `SDK -> SDM route -> Storage leader -> Raft/WAL/KV`。本页只放入口和摘要，完整数据见 put、get、mixed 三份报告。

## 运行方式

项目使用 `tools/bench/bench_client` 作为 benchmark client，并通过 `scripts/bench.sh` 自动拉起本地集群后运行。默认本地集群为：

```text
1 meta + 1 sdm + 5 storage
```

单次 benchmark 示例：

```bash
BENCH_LOG_LEVEL=warning \
  ./scripts/bench.sh \
  --workload=mixed \
  --read_ratio=0.80 \
  --threads=16 \
  --shard_count=2 \
  --replica_count=3 \
  --value_size=128 \
  --requests=30000 \
  --output_json=build/release/bench_results/mixed_baseline.json
```

## 本次结果

测试环境：`Mac15,7`，Apple M3 Pro，12 物理核心 / 12 逻辑 CPU，36 GiB 内存；macOS 15.7.4，arm64。本地集群：`1 meta + 1 sdm + 5 storage`，所有进程都运行在同一台机器上，并通过 `127.0.0.1/localhost` 通信。

默认场景：

```text
threads       = 16
shard_count   = 2
replica_count = 3
value_size    = 128
requests      = 30000
key_count     = 1000
```

| Workload | Scenario | success_qps | avg_us | p95_us | p99_us |
|---|---|---:|---:|---:|---:|
| put | baseline | 9799.99 | 1631.30 | 2577 | 5623 |
| get | baseline | 11059.01 | 1445.76 | 1905 | 2211 |
| mixed | read_ratio=0.80 | 8229.54 | 1942.99 | 3358 | 4474 |

## 详细结果

- [Put benchmark](benchmark_put.md)
- [Get benchmark](benchmark_get.md)
- [Mixed benchmark](benchmark_mixed.md)

原始 CSV 数据位于 [benchmark_results](benchmark_results)。图表 SVG 位于 [assets](assets)。
