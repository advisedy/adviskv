
# AdvisKV

直接拿AI跑的README

AdvisKV 是一个基于 C++17、gRPC、Raft 的分布式 KV 项目。当前仓库聚焦两条主线：

- `Storage`：真正承载数据、副本、Raft 复制与恢复。
- `SDM`：作为控制面，负责节点注册、心跳接收、表放置、路由维护，以及 `create_table` 的工作流推进。

结合当前代码进度，这个项目已经不再是“只有几个后台扫描任务”的雏形，而是逐步收敛成一个有清晰控制面 / 数据面边界的系统。最近一轮实现重点落在 `Meta -> SDM -> Storage` 的建表主链路，以及 SDM 里的 `PlaceTableWorkflow`。

## 项目目标

- 以 `table` 为单位做分片和副本管理。
- 用 Raft 维护每个 shard 的副本一致性。
- 通过 SDM 统一管理节点、路由和建表流程。
- 让 SDM 的价值集中在“工作流 + 调度 + 元数据”而不是分散在各种补洞式后台任务里。

## 当前架构

### 1. Meta

`Meta` 是元数据入口，当前主要负责 DDL 类请求，尤其是 `CreateTable`：

- 接收上层创建表请求。
- 本地维护 catalog 元数据。
- 调用 SDM 的 `PlaceTable`，让 SDM 负责后续放置和副本创建。

入口文件：

- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/meta/main/main.cpp)

默认监听：

- `50052`

### 2. SDM

`SDM` 是当前项目的控制面核心，主要负责：

- `RegisterNode`：Storage 节点注册。
- `HeartBeat`：接收节点和副本状态上报。
- `PlaceTable`：驱动建表工作流。
- `GetRoute`：提供分片路由查询。
- 后台维护 `ShardRoute`。

当前 SDM 的实现重点是 `PlaceTableWorkflow`。它已经不是一把梭的同步函数，而是一个可逐步推进的状态机：

- `CREATING`
- `PLACING`
- `CREATING_REPLICAS`
- `WAITING_READY`
- `WAITING_ROUTE_READY`
- `ROLLING_BACK`
- `READY`
- `FAILED`

这些状态定义在：

- [store.h](file:///Users/bytedance/Desktop/adviskv/src/sdm/model/store.h)

核心流程代码：

- [placetable_workflow.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/workflow/placetable_workflow.cpp)
- [placetable_workflow_runner.h](file:///Users/bytedance/Desktop/adviskv/src/sdm/workflow/placetable_workflow_runner.h)
- [routeupdate_check_task.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/background/routeupdate_check_task.cpp)

当前语义是：

- `TableService` 只负责把请求转成 `Table` 领域对象并触发 workflow。
- workflow 负责重名检测、placement、创建 replica、等待 ready、失败回滚。
- route 不由 workflow 手动双写，而是由 `RouteUpdateCheckTask` 统一维护。

入口文件：

- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/main/main.cpp)

默认监听：

- `50051`

### 3. Storage

`Storage` 是数据面，负责：

- `Put/Get/Delete`
- `CreateReplica`
- Raft RPC：`RequestVote`、`AppendEntries`
- 本地 replica 生命周期管理
- 启动时恢复已有副本
- 通过 `NodeAgent` 向 SDM 注册并周期心跳

入口文件：

- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/storage/main/main.cpp)

服务端关键实现：

- [storage_service.cpp](file:///Users/bytedance/Desktop/adviskv/src/storage/handler/storage_service.cpp)

默认监听：

- `50050`

## 当前主链路

当前仓库最重要的一条链路是 `create_table`：

1. 上层请求调用 Meta 的 `CreateTable`。
2. Meta 调用 SDM 的 `PlaceTable`。
3. SDM 在 `PlaceTableWorkflow` 中创建 table 元数据，并进入 `PLACING`。
4. SDM 根据 `resource_pool` 选择节点，为每个 shard 写入 `PENDING` replica。
5. SDM 通过 `StorageClient` 向目标 Storage 发送 `CreateReplica`。
6. Storage 收到请求后创建本地 replica，并启动对应的 Raft 组。
7. Storage 通过 `NodeAgent` 向 SDM 上报 heartbeat。
8. SDM 根据 heartbeat 把 replica 状态从 `ADDING` 推进到 `READY`。
9. `RouteUpdateCheckTask` 根据健康副本生成 `ShardRoute`。
10. workflow 观察到 route 就绪后把 table 推进到 `READY`。

这条链路的关键点是：

- `CreateReplica OK` 只表示 Storage 已接单，不表示副本已经 ready。
- replica 的真正 ready 依赖后续 heartbeat 异步上报。
- route 的生成是后台维护完成的，workflow 只等待 route ready。
- 建表失败会进入 `ROLLING_BACK`，回滚成功后删除 table，避免残留同名脏状态。

还有我忘了补充了，我这个项目的有一条最基本的链路就是，还有一个sdk库，负责提供给外部的， 客户端可以使用client->search_kv()这种形式，首先会传递db和table和key，然后这个db和table会直接在sdm那边找到对应的路由表（这部分我不打算先通过查找db和table和key在对应的哪一个shard，然后再发送给sdm去超找到对应的shard的路由表，因为我觉得太麻烦了，反而没必要），然后发送给对应的storage去查询key，大概就是这个形式。 

## 设计取舍

### 为什么要做 workflow

`PlaceTable` 不是一个单纯的写元数据动作，它至少包含：

- 重名校验
- 选择节点
- 写 replica 元数据
- 向多个 storage 发 `CreateReplica`
- 等待异步 ready
- 等待 route ready
- 失败回滚

如果这些逻辑散在后台任务里，会有几个明显问题：

- 职责不清，主链路不闭环。
- 中间态难以表达。
- 崩溃恢复语义混乱。
- 重试和幂等边界不清楚。

因此当前 SDM 采用的是“以 `TableLifecycle` 为核心”的 workflow 推进模型。

### 当前已收敛的 SDM 职责

- `TableService`：保留为 table 领域入口，但尽量保持薄。
- `PlaceTableWorkflow`：承载建表主逻辑。
- `PlaceTableWorkflowRunner`：后台推进未终态 table。
- `RouteUpdateCheckTask`：唯一负责维护 route。
- `StorageClient`：把 SDM 内部参数转换为 gRPC 请求，避免 workflow 直接依赖 protobuf 细节。

## 仓库结构

```text
adviskv/
├── conf/                 # 默认配置
├── docs/                 # 设计文档与开发记录
├── proto/                # gRPC / protobuf 协议
├── scripts/              # 构建脚本
├── src/
│   ├── common/           # 通用组件
│   ├── meta/             # Meta 服务
│   ├── sdm/              # 控制面
│   └── storage/          # 数据面
├── third_party/vcpkg/    # 依赖管理
└── README.md
```

## 依赖

项目当前通过 `vcpkg + CMake` 管理依赖，根目录 `CMakeLists.txt` 依赖：

- `fmt`
- `spdlog`
- `protobuf`
- `gRPC`
- `yaml-cpp`

编译标准：

- `C++17`

## 构建

### 1. 配置并编译

仓库已经提供了一个最简单的构建脚本：

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

当前仓库的 protobuf 生成代码使用 `build/generated` 目录。如果是一个全新工作区，先执行：

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

- storage 启动后会通过 `NodeAgent` 向 SDM 注册并发送 heartbeat。
- meta 启动后会持有 SDM client，后续建表直接走主链路。

## 当前实现状态

### 已经完成的部分

- `Meta -> SDM -> Storage` 的建表主链路已接通。
- Storage 侧的 `CreateReplica` 已实现基本幂等处理。
- SDM 已经引入 `PlaceTableWorkflow`。
- workflow 已支持：
  - duplicate table name 检测
  - placement
  - create replica
  - waiting replica ready
  - waiting route ready
  - rollback
- route 维护职责已统一收口到 `RouteUpdateCheckTask`。
- `StorageClient` 已抽出内部参数 `CreateReplicaParam`，workflow 不直接拼 protobuf。

### 目前仍然简化的地方

- `SdmStore` 现在还是内存版，真正的持久化 metastore 还没落地。
- 因为 metastore 还没持久化，SDM 重启后的“恢复续跑”还只是结构已准备好，能力还没有完全闭环。
- `delete_table`、`retry_failed_table`、`list_tables` 这类 table 领域接口还没补齐。
- 当前本地配置只提供了一个 `storage-1` 节点，更多副本 / 多机位演示还需要继续补配置。


## 接下来最值得做的事

如果继续往下推进，最优先的下一步是：

1. 实现持久化版 `SdmStore` / `ISdmMetaStore`
2. 验证 SDM 重启后 workflow 能继续推进到终态
3. 补 `delete_table` 等 table 领域接口
4. 增加多 storage 节点配置，验证多副本和 route 更新

## 相关文件

如果想从代码入口开始看，建议按这个顺序：

- [README.md](file:///Users/bytedance/Desktop/adviskv/README.md)
- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/meta/main/main.cpp)
- [main.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/main/main.cpp)
- [placetable_workflow.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/workflow/placetable_workflow.cpp)
- [table_service.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/service/table_service.cpp)
- [routeupdate_check_task.cpp](file:///Users/bytedance/Desktop/adviskv/src/sdm/background/routeupdate_check_task.cpp)
- [storage_service.cpp](file:///Users/bytedance/Desktop/adviskv/src/storage/handler/storage_service.cpp)
- [store.h](file:///Users/bytedance/Desktop/adviskv/src/sdm/model/store.h)

## 一句话总结

AdvisKV 当前已经从“只靠后台任务凑主链路”的阶段，走到了“以 workflow 驱动 table 生命周期”的阶段。Storage 负责 Raft 和数据正确性，SDM 负责建表工作流、路由和元数据，这也是这个项目接下来继续深化的主方向。
