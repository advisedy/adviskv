# AdvisKV


(直接拿AI临时跑了个README出来)

AdvisKV 是一个基于 `C++17 + gRPC + Raft` 的分布式 KV 项目。

这份 README 采用更保守、也更贴近当前代码现状的写法：它不是把项目包装成“已经完成的生产级数据库”，而是把项目真实的定位、模块边界、当前完成度，以及接下来最值得做的事写清楚。

TODO:

[ok] meta侧的持久化还没有完成

[] storage的发送raft消息等待优化

[ok] mian函数里面的监听ip 等待之后统一

[] storage里面还没有集成rocksdb

[] meta的本地备份 ：每次写入前保留旧版本备份，防止磁盘静默损坏

[] SDM Store 读接口后续改成 snapshot value 语义：`get_xxx` 返回 `std::optional<T>`（例如 `using ReplicaOr = std::optional<Replica>`），`list_xxx` 返回 `std::vector<T>`，避免把内部 mutable `shared_ptr` 暴露给调用方；所有修改必须通过 `put_xxx/delete_xxx` 进入 Store，以保证持久化和 runtime index 更新边界一致。

[ok] 补全单测文件 storage 

[] 补全单测文件 meta

[] 补全单测文件 sdm

[] 线性一致性读 ReadIndex




## 项目定位

AdvisKV 当前更准确的定位是：

- 一个自研分布式 KV 系统项目
- 一个正在把主链路、控制面边界、数据面正确性逐步做实的工程
- 一个以 `Storage` 和 `SDM` 两条主线为核心的分布式系统实现

当前项目最值得关注的三条主线是：

- `Storage`：Raft、一致性复制、WAL、snapshot、恢复
- `SDM`：建表部署 workflow、placement、route、节点与副本状态观测
- 外部 `SDK` 路径：`db + table + key -> SDM 查路由 -> Storage 读写`

当前项目还不是：

- 完整高可用数据库
- 成熟的动态扩缩容系统
- 已经具备 config-change / rebalance / 控制面 HA 的生产级系统

## 模块边界

### 1. Meta

`Meta` 的职责应当理解为：

- DDL 入口
- catalog / 命名空间权威层
- `db_id / table_id` 分配者

当前它主要负责：

- 接收 `CreateDB / CreateTable / GetTable`
- 维护本地 catalog 元数据
- 调用 SDM 的 `PlaceTable`

它不应负责：

- placement
- route
- replica 生命周期
- Raft 内部状态

当前入口：

- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/meta/main/main.cpp)

默认监听：

- `50052`

### 2. SDM

`SDM` 是当前项目的控制面，定位应当是：

- deployment workflow
- route 管理
- placement
- 节点注册与状态观测

当前已经承载的职责包括：

- `RegisterNode`
- `HeartBeat`
- `PlaceTable`
- `GetRoute`
- 后台推进 `PlaceTableWorkflow`
- 后台维护 `ShardRoute`

当前 SDM 的重点不应该是“后台任务数量很多”，而应该是下面几件事是否闭环：

- 如何决定节点放置
- 如何推进建表部署状态
- 如何根据 heartbeat 更新副本状态
- 如何维护 route
- 如何为 SDK 提供统一路由入口

当前入口：

- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/main/main.cpp)

默认监听：

- `50051`

### 3. Storage

`Storage` 是数据面，也是当前项目最核心的技术模块。

它负责：

- `Put / Get / Delete`
- `CreateReplica`
- Raft RPC：`RequestVote / AppendEntries / InstallSnapshot`
- 本地 replica 生命周期管理
- WAL / snapshot / raft_meta 恢复
- 通过 `NodeAgent` 向 SDM 注册并周期心跳

当前入口：

- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/storage/main/main.cpp)

关键实现：

- [storage_service.cpp](file:///Users/bytedance/Desktop/adviskv/src/storage/handler/storage_service.cpp)

默认监听：

- `50050`

### 4. 外部 SDK

项目后续会保留一条明确的外部调用路径：

- 客户端调用 SDK
- SDK 传入 `db + table + key`
- SDK 向 SDM 调 `GetRoute`
- SDM 根据名字和 key 计算目标 shard
- SDK 再访问对应 Storage

当前协议层已经支持这条路径：

- `proto/sdm.proto` 中已有 `GetRouteRequest { db_name, table_name, key }`

这个方向的优点是：

- SDK 接口更简单
- 路由规则集中在 SDM
- 将来如果分片规则变化，不需要修改 SDK API

## 当前主链路

### 1. 建表链路

当前仓库最重要的一条链路是 `CreateTable`：

1. 上层请求调用 Meta 的 `CreateTable`
2. Meta 调用 SDM 的 `PlaceTable`
3. SDM 在 `PlaceTableWorkflow` 中创建控制面对象并进入部署流程
4. SDM 根据 `resource_pool` 选择节点，为每个 shard 写入 `PENDING` replica
5. SDM 通过 `StorageClient` 向目标 Storage 发送 `CreateReplica`
6. Storage 收到请求后创建本地 replica，并启动对应的 Raft group
7. Storage 通过 `NodeAgent` 向 SDM 上报 heartbeat
8. SDM 根据 heartbeat 把 replica 状态从 `ADDING` 推进到 `READY`
9. `RouteUpdateCheckTask` 根据健康副本生成 `ShardRoute`
10. workflow 观察 route 就绪后把 table 推进到终态

这条链路当前已经是项目的主心骨。

### 2. 读写链路

项目目标中的对外读写链路是：

1. SDK 接收 `db + table + key`
2. SDK 向 SDM 发送 `GetRoute`
3. SDM 根据 `db_name + table_name + key` 定位 shard
4. SDM 返回 route
5. SDK 调用对应 Storage 做 `Put / Get`

目前这条链路在协议和基本实现上是成立的，但仍然存在一个需要继续补强的问题：

- 当前 `GetRoute` 返回语义还不够强
- 代码里仍有“直接取 `route.replicas[0]`”的简化逻辑
- 这意味着“返回的一定是 leader”这件事还没有被严格保证

所以，当前读写路径的方向已经对了，但 correctness 还不算完全闭环。

## 当前设计取舍

### 为什么保留 Meta

`Meta` 很薄，但这是合理的薄。

这个项目里，`Meta` 更适合做：

- catalog
- DDL 入口
- ID 分配

而不是去承担：

- route 管理
- 副本调度
- 节点观测

把 `Meta` 保持成一个边界清楚的薄层，比强行把它做重更合理。

### 为什么保留 SDM

如果 `SDM` 只是：

- 节点注册
- 心跳接收
- 路由缓存

那它确实会显得偏薄。

但如果 `SDM` 承担的是：

- 建表部署 workflow
- placement
- route 维护
- 节点与副本状态观测
- 面向 SDK 的统一路由入口

那它就是一个成立的控制面，而不是鸡肋。

### 为什么 SDK 不自己算 shard

当前项目倾向保留这样的接口：

- `search_kv(db, table, key)`
- `put_kv(db, table, key, value)`

也就是说：

- 客户端不先自己算 shard
- 客户端统一把路由定位交给 SDM

这样做的原因是：

- 降低 SDK 复杂度
- 集中路由逻辑
- 为后续分片规则变化留空间

## 当前完成度

### 已经具备的部分

- `Meta -> SDM -> Storage` 的建表主链路已经接通
- `StorageService::CreateReplica` 已有基本实现
- `SDM` 已经引入 `PlaceTableWorkflow`
- workflow 已覆盖以下关键阶段：
  - 重名检测
  - placement
  - 创建 replica
  - 等待 replica ready
  - 等待 route ready
  - rollback
- `RouteUpdateCheckTask` 已经承担 route 维护职责
- `GetRoute(db_name, table_name, key)` 的协议与基础逻辑已经存在
- `Storage` 侧已经有 Raft RPC 和持久化相关代码骨架

### 当前仍然简化或未闭环的部分

- `ISdmMetaStore` 目前仍是内存版
- SDM 重启后的 workflow 恢复还没有真正闭环
- route 目前还没有严格保证返回 leader 可写路由
- 外部 SDK 还没有作为完整独立模块落地
- 多节点、多副本的本地演示配置还不完整
- benchmark 还没有形成正式结果
- 崩溃恢复、故障切换还没有形成完整的实验与证据链

### 客观评价

当前项目已经不是“几个类拼出来的玩具”了，但也还没有达到“成熟分布式系统作品”的程度。

更准确地说，它现在处于：

- 基础骨架已经搭对
- 模块边界逐渐清晰
- 关键主线已经出现
- 但最有说服力的可靠性与性能证据还没补齐

## 当前最值得优先做的事

如果目标是把这个项目做成一个更有说服力的秋招项目，优先级建议如下：

1. 修正 `GetRoute` 语义，明确 leader 路由
2. 跑通 SDK -> SDM -> Storage 的完整读写链路
3. 补齐多节点、多副本本地演示配置
4. 验证 leader 宕机后的重新选主与继续读写
5. 补 `WAL / snapshot / raft_meta` 的恢复实验
6. 做第一版 benchmark
7. 再补 `ISdmMetaStore` 的持久化与 workflow 重启续跑

对当前阶段来说，可视化不应排在前面。

## 当前不打算在 V1 解决的事

以下内容暂时不应抢占主工期：

- dashboard / 可视化大屏
- SDM HA
- Raft config-change
- shard rebalance
- 复杂的多策略 leader selector
- 过早的平台化能力

原因：

- 这些内容会快速拉高工期
- 但对当前项目“正确性、恢复、性能”的核心说服力帮助有限

## 仓库结构

```text
adviskv/
├── conf/                 # 默认配置
├── docs/                 # 设计文档与开发记录
├── proto/                # gRPC / protobuf 协议
├── scripts/              # 构建脚本
├── src/
│   ├── common/           # 通用组件
│   ├── meta/             # Meta / catalog / DDL 入口
│   ├── sdm/              # 控制面：workflow / route / placement / heartbeat
│   └── storage/          # 数据面：replica / raft / persist / rpc
├── third_party/vcpkg/    # 依赖管理
└── README.md
```

## 依赖

项目当前通过 `vcpkg + CMake` 管理依赖，核心依赖包括：

- `fmt`
- `spdlog`
- `protobuf`
- `gRPC`
- `yaml-cpp`

编译标准：

- `C++17`

## 构建

### 1. 配置并编译

仓库已提供基础构建脚本：

```bash
./scripts/build.sh
```

它等价于：

```bash
cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE=third_party/vcpkg/scripts/buildsystems/vcpkg.cmake

cmake --build build
```

编译产物默认位于：

- `build/bin/meta`
- `build/bin/sdm`
- `build/bin/storage`

### 2. 首次生成 protobuf 代码

如果是全新工作区，先执行：

```bash
mkdir -p build/generated

./build/vcpkg_installed/arm64-osx/tools/protobuf/protoc \
  -I=proto \
  --cpp_out=./build/generated \
  --grpc_out=./build/generated \
  --plugin=protoc-gen-grpc=./build/vcpkg_installed/arm64-osx/tools/grpc/grpc_cpp_plugin \
  proto/common.proto \
  proto/storage.proto \
  proto/meta.proto \
  proto/sdm.proto
```

然后再执行构建脚本。

## 本地运行

当前默认配置文件：

- `Meta`：`conf/meta.yaml`
- `SDM`：`conf/sdm.yaml`
- `Storage`：`conf/storage-1.yaml`

默认端口：

- `Storage`：`50050`
- `SDM`：`50051`
- `Meta`：`50052`

建议在仓库根目录分别启动三个进程，因为目前配置文件使用的是相对路径：

```bash
./build/bin/sdm
```

```bash
./build/bin/storage
```

```bash
./build/bin/meta
```

推荐启动顺序：

1. `sdm`
2. `storage`
3. `meta`

原因：

- `storage` 启动后会向 `SDM` 注册并发送 heartbeat
- `meta` 启动后会持有 SDM client，后续 DDL 直接走主链路

## 建议的阅读顺序

如果想从代码入口开始理解项目，建议按下面的顺序看：

1. [README.md](file:///Users/bytedance/Desktop/adviskv/README.md)
2. [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/meta/main/main.cpp)
3. [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/main/main.cpp)
4. [placetable_workflow.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/workflow/placetable_workflow.cpp)
5. [routeupdate_check_task.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/background/routeupdate_check_task.cpp)
6. [route_service.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/service/route_service.cpp)
7. [storage_service.cpp](file:///Users/bytedance/Desktop/adviskv/src/storage/handler/storage_service.cpp)
8. [raft_node.cpp](file:///Users/bytedance/Desktop/adviskv/src/storage/raft/raft_node.cpp)

## 一句话总结

AdvisKV 当前最真实的状态是：

- 架构方向已经逐步收敛
- `Meta / SDM / Storage` 的边界比之前更清楚
- 主链路已经出现
- 但真正决定项目上限的，接下来仍然是 `Storage` 的恢复与正确性、SDK 路由链路的可用性、以及 `SDM` 的持久化闭环

也就是说，这个项目当前最需要的不是更多概念，而是更强的证据。
