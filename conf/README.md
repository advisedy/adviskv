# conf

- `catalog.yaml` / `topo.yaml` / `storage-*.yaml`：拉起 demo 集群用的
- `client.yaml`：`adviskvctl` 用的，去连接 demo 集群
- `test.yaml`：单测用的

里面的 `data_dir`、`log_dir` 都是相对项目根目录的路径。

```text
build/
├── bin/
├── demo/         # demo
│   ├── data/
│   └── logs/
├── unit_test/    # 单测
│   ├── data/
│   └── logs/
├── e2e_test/     # e2e
│   ├── conf/     # 跑的时候生成的
│   ├── data/
│   └── logs/
└── bench/        # 压测
    └── <run_id>/
        ├── conf/
        ├── data/
        └── logs/
```
