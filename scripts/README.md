# Scripts


路径约定：依赖在 `.adviskv_deps/`（`setup.sh`），工程默认输出在 `build/`（默认 Debug）。
本地运行目录：`build/demo`、`build/unit_test`、`build/e2e_test`、`build/bench`（可以看 [`conf/README.md`](../conf/README.md)）。
特例：`build-asan/`、`build-coverage/`。
这个目录是常用的一些脚本。

 `internal/` 里面的脚本基本是为了服务给这个目录里面的脚本使用的，可以不需在意。

| 脚本 | 用途 |
| --- | --- |
| `setup.sh` | 准备本地依赖；使用 `--with-maelstrom-test` 时额外安装 Maelstrom 测试工具。 |
| `build.sh` | 构建项目；可通过 `BUILD_TARGETS=...` 做局部构建。 |
| `run_test.sh` | 构建 + C++ 测试；检测到 Maelstrom 时额外运行 3 节点正常测试和 5 节点故障注入压力测试。 |
| `e2e_pytest.sh` | 构建 + e2e测试 |
| `coverage.sh` | 执行覆盖率测试 |
| `bench.sh` | 自动拉起集群（配置来自conf目录）并运行 `bench_client`。 |
| `bench_metrics.sh` | `bench.sh` + 生成 `metrics` 报告 |
| `adviskvctl_demo.sh` | 一个交互式demo，会自动拉起集群，可输入 `create_db`, `create_table`, `put`, `get` ... |
| `stop_cluster.sh` | 停止拉起来的集群 |

`asan/` 是测试脚本 + ASAN + UBSAN
