# Maelstrom Raft

用 [Maelstrom](https://github.com/jepsen-io/maelstrom) 验证 AdvisKV 的 `RaftCore` 的线性一致性。

## 准备

```bash
./scripts/setup.sh --with-maelstrom-test   # 需要 JDK 17+
./scripts/build.sh                         # 产出 build/bin/maelstrom_raft_node
```

## 运行

```bash
# 无故障
./scripts/internal/run_maelstrom_raft.sh

# 网络分区注入
./scripts/internal/run_maelstrom_raft.sh --nemesis partition --time-limit 60
```

结果：`tools/maelstrom_raft/store/latest/`  
节点数据：`$ADVISKV_MAELSTROM_DATA`（默认放到了 `/tmp/adviskv_maelstrom`）

`./scripts/run_test.sh` 若检测到本机 Maelstrom，会在 C++ 测试后自动跑一轮无故障 + 一轮 `--nemesis partition` 压力测试。
