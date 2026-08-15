# v0.1.0 SDK 写请求重试验收

## 1. 单次 RPC 超时后的不确定写结果

### 测试条件

- Release 构建，1 shard、3 replicas。
- PUT，16 个并发线程，每轮 5000 个请求，value 5000 bytes。
- `sdk_rpc_timeout_ms=120`，`sdk_write_timeout_ms=3000`，`sdk_retry_delay_ms=10`。
- 其他参数一致，只比较 `max_retries=0` 和 `max_retries=1`。

### 结果

| 配置 | 成功 | 最终失败/`UNKNOWN` | 失败率 |
|---|---:|---:|---:|
| `max_retries=0` | 2464 / 5000 | 2536 / 5000 | 50.72% |
| `max_retries=1` | 4944 / 5000 | 56 / 5000 | 1.12% |

在这个固定压力场景中，最终失败率下降 49.60 个百分点

### 测试命令

不重试：

```bash
BUILD_DIR=build-release \
BUILD_TYPE=Release \
SKIP_BUILD=1 \
./scripts/bench_metrics.sh \
  --workload=put \
  --threads=16 \
  --shard_count=1 \
  --replica_count=3 \
  --value_size=5000 \
  --key_count=1000 \
  --requests=5000 \
  --warmup_requests=0 \
  --sdk_rpc_timeout_ms=120 \
  --sdk_write_timeout_ms=3000 \
  --sdk_read_timeout_ms=3000 \
  --sdk_max_retries=0 \
  --sdk_route_max_retries=1 \
  --sdk_retry_delay_ms=10
```

开启 1 次重试：

```bash
BUILD_DIR=build-release \
BUILD_TYPE=Release \
SKIP_BUILD=1 \
./scripts/bench_metrics.sh \
  --workload=put \
  --threads=16 \
  --shard_count=1 \
  --replica_count=3 \
  --value_size=5000 \
  --key_count=1000 \
  --requests=5000 \
  --warmup_requests=0 \
  --sdk_rpc_timeout_ms=120 \
  --sdk_write_timeout_ms=3000 \
  --sdk_read_timeout_ms=3000 \
  --sdk_max_retries=1 \
  --sdk_route_max_retries=1 \
  --sdk_retry_delay_ms=10
```

对应的 metrics report：

- [`retry_metrics_rpc120_r0_5000.txt`](retry_metrics_rpc120_r0_5000.txt)
- [`retry_metrics_rpc120_r1_5000.txt`](retry_metrics_rpc120_r1_5000.txt)

## 2. leader 崩溃后的故障切主

### 测试条件

- Release 构建，1 shard、3 replicas。
- PUT，4 个并发线程，每轮 800 个请求，warmup 100，value 128 bytes。
- 测试过程中 kill 当前 leader 一次。
- `sdk_rpc_timeout_ms=100`，`sdk_write_timeout_ms=3000`，`sdk_retry_delay_ms=20`。
- 比较 `max_retries=1` 和 `max_retries=3`。

### 结果

### `max_retries=1`

| 轮次 | kill 后请求 | 成功 | 失败/`UNKNOWN` | 失败率 |
|---:|---:|---:|---:|---:|
| 1 | 784 | 0 | 784 | 100.00% |
| 2 | 782 | 0 | 782 | 100.00% |
| 合计 | 1566 | 0 | 1566 | 100.00% |

### `max_retries=3`

| 轮次 | kill 后请求 | 成功 | 失败/`UNKNOWN` | 失败率 | 首次恢复窗口 |
|---:|---:|---:|---:|---:|---:|
| 1 | 796 | 496 | 300 | 37.69% | 1688 ms |
| 2 | 796 | 555 | 241 | 30.28% | 1382 ms |
| 合计 | 1592 | 1051 | 541 | 33.98% | — |

前两轮合计中，业务方看到的失败率从 `100.00%` 降到 `33.98%`，下降 66.02 个百分点

### 测试命令

`max_retries=1`：

```bash
BUILD_DIR=build-release \
BUILD_TYPE=Release \
SKIP_BUILD=1 \
./scripts/failover_bench.sh \
  --shard-count=1 \
  --replica-count=3 \
  --workload=put \
  --threads=4 \
  --requests=800 \
  --warmup-requests=100 \
  --key-count=1000 \
  --value-size=128 \
  --sdk-rpc-timeout-ms=100 \
  --sdk-write-timeout-ms=3000 \
  --sdk-read-timeout-ms=3000 \
  --max-retries=1 \
  --route-max-retries=1 \
  --retry-delay-ms=20 \
  --route-refresh-interval-ms=60000
```

`max_retries=3`：

```bash
BUILD_DIR=build-release \
BUILD_TYPE=Release \
SKIP_BUILD=1 \
./scripts/failover_bench.sh \
  --shard-count=1 \
  --replica-count=3 \
  --workload=put \
  --threads=4 \
  --requests=800 \
  --warmup-requests=100 \
  --key-count=1000 \
  --value-size=128 \
  --sdk-rpc-timeout-ms=100 \
  --sdk-write-timeout-ms=3000 \
  --sdk-read-timeout-ms=3000 \
  --max-retries=3 \
  --route-max-retries=1 \
  --retry-delay-ms=20 \
  --route-refresh-interval-ms=60000
```

归档的原始 JSON：

- `max_retries=1`：
  - [`retry_failover_r1.json`](retry_failover_r1.json)
  - [`retry_failover_r1_2.json`](retry_failover_r1_2.json)
- `max_retries=3`：
  - [`retry_failover_r3.json`](retry_failover_r3.json)
  - [`retry_failover_r3_2.json`](retry_failover_r3_2.json)

## 注意

由于后来换了机子，目前机子是2核6G，所以QPS和benchmark有误，是正常现象。