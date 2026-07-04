# AdvisKV

<p align="center">
  <img src="docs/assets/adviskv.jpeg" alt="AdvisKV banner" width="960" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white" alt="C++17" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License" /></a>
  <img src="https://img.shields.io/badge/Platform-macOS%20%7C%20Linux-2ea44f" alt="Platform macOS and Linux" />
  <img src="https://img.shields.io/badge/Version-v0.1-blue" alt="Version v0.1" />
</p>

AdvisKV 是一个使用 **C++17 + gRPC + Protobuf + Raft** 实现的分布式 Key-Value 存储项目。项目采用控制面与数据面拆分的架构：`Meta` 负责 DDL 与 catalog，`SDM` 负责部署、路由、节点与副本状态管理，`Storage` 负责 KV 数据读写、Raft 复制和本地持久化，`SDK` 对外提供基于 `db + table + key` 的访问接口。

当前项目是一个可构建、可运行、可测试、持续演进中的分布式 KV 系统原型。它已经具备建库、建表、路由解析、KV 读写、Raft 副本、WAL、snapshot、恢复、单测、E2E 和 benchmark 客户端等主干能力；尚未完成控制面高可用、动态扩缩容、Raft config change、自动 rebalance 等生产级能力。

## 目录

- [项目简介](#项目简介)
- [功能特性](#功能特性)
- [整体架构](#整体架构)
- [技术栈](#技术栈)
- [安装指南](#安装指南)
- [快速开始](#快速开始)
- [API 文档与使用说明](#api-文档与使用说明)
- [配置说明](#配置说明)
- [项目结构](#项目结构)
- [测试](#测试)
- [Benchmark 测试结果](#benchmark-测试结果)
- [Benchmark 工具](#benchmark-工具)
- [开发状态与限制](#开发状态与限制)
- [许可证信息](#许可证信息)

## 项目简介

AdvisKV 主要解决分布式 KV 存储中的以下问题：

- 通过统一 DDL 入口创建数据库和表。
- 将表拆分为多个 shard，并为每个 shard 部署多个 replica。
- 维护节点、replica、route、table phase 等控制面状态。
- 通过 SDK 将 `db + table + key` 路由到正确的 Storage 节点。
- 在 Storage 内部通过 Raft 实现副本复制、leader 写入、日志提交、状态机 apply、WAL、snapshot 和恢复。

项目特点：

- **控制面/数据面拆分**：`Meta` 管 catalog，`SDM` 管 placement 和 route，`Storage` 管数据与 Raft。
- **表级分片与副本模型**：建表时可配置 `shard_count`、`replica_count` 和 `resource_pool`。
- **Raft 副本复制**：Storage 内部实现投票、日志复制、commit/apply、snapshot 安装和 follower catch-up。
- **统一 SDK 访问路径**：客户端只需要关心 `db_name`、`table_name` 和 `key`。
- **本地工具链完整**：提供构建脚本、CLI、C++ E2E、pytest E2E、CTest、coverage 和 benchmark 工具。
- **模块边界清晰**：各模块以独立 library/executable 组织，proto 协议和 C++ 实现分层明确。

## 功能特性

### 数据库与表管理

- `CreateDB`：创建数据库命名空间。
- `CreateTable`：创建表，并指定 shard 数、副本数和资源池。
- `DropTable`：删除表，触发控制面删除流程。
- `GetTable`：查询表元信息和当前状态。

### Catalog 与持久化

- Meta 维护 `db_id`、`table_id`、表状态、operation id 等 catalog 元数据。
- Meta 使用本地 `MetaPersistEngine` 保存 catalog 数据，支持主干状态重启恢复。
- SDM 使用 persistent metastore 保存表、节点、replica、route 等控制面元数据。
- Storage 持久化 WAL、snapshot 和 replica meta。

### SDM 控制面

- Storage 节点注册与心跳上报。
- table placement 与 replica 期望状态生成。
- table reconciler 后台推进建表/删表流程。
- heartbeat check 后台维护节点状态。
- route update 后台根据健康 replica 生成 shard route。
- 根据 `db_name + table_name + key` 提供统一 route 查询。

### Storage 数据面

- 支持 `Put`、`Get`、`Delete` KV 操作。
- 支持 `CreateReplica`、`DeleteReplica`、`GetReplicaInfo` replica 生命周期接口。
- 支持 Raft RPC：`RequestVote`、`AppendEntries`、`InstallSnapshot`。
- `ReplicaManager` 负责本地副本创建、恢复和 tick 调度。
- 内置 map-based KV engine，便于验证一致性与复制逻辑。
- 支持 metrics HTTP 暴露，默认 Storage 配置为 `127.0.0.1:51051/metrics`。

### SDK

- 对外提供 `KVClient::put`、`KVClient::get`、`KVClient::del`。
- 自动向 SDM 解析 route。
- 遇到 `NOT_LEADER`、`REPLICA_NOT_FOUND`、`ROUTE_NOT_FOUND` 时重新解析 route 并重试一次。
- 支持 SDK 日志回调与基础 metrics 计数。

### 测试与工具

- GoogleTest 单测覆盖 common、Meta、SDM、Storage、Raft、persist、replica、selector、background task 等模块。
- C++ E2E 覆盖 basic KV、重启恢复、leader failover、follower catch-up、SDM/Meta crash recovery 等场景。
- Python pytest E2E 脚本负责拉起/停止本地集群并执行端到端测试。
- coverage 脚本可生成 HTML/XML 覆盖率报告。
- benchmark client 支持 put/get/mixed workload、并发线程、JSON 输出和 metrics 暴露。

## 整体架构

```text
Client / CLI / Benchmark
        |
        v
      SDK
        |
        | GetRoute(db, table, key)
        v
       SDM  <-------------------- Storage NodeAgent HeartBeat
        |
        | PlaceTable / route / node / replica control plane
        v
      Meta  -- DDL/catalog authority

DDL path:
Meta CreateTable -> SDM PlaceTable -> Storage CreateReplica -> HeartBeat -> Route Ready

Data path:
SDK -> SDM GetRoute -> Storage Put/Get/Delete -> Raft -> KV StateMachine
```

核心模块：

- **Meta**：DDL 入口和 catalog 权威层，负责 `CreateDB`、`CreateTable`、`DropTable`、`GetTable`，并把表部署请求转交给 SDM。
- **SDM**：Sharding/Deployment Manager，负责节点注册、心跳、资源池、placement、replica 期望状态、route 维护和 table phase 推进。
- **Storage**：数据面服务，负责 KV RPC、replica 生命周期、Raft RPC、WAL/snapshot/恢复和本地状态机。
- **SDK**：用户侧访问库，隐藏 route 解析和 Storage RPC 细节。
- **Common**：日志、配置、状态码、类型定义、metrics、线程池、后台任务、路径工具等公共组件。

## 技术栈

- **语言**：C++17。
- **构建系统**：CMake 3.20+，默认使用 Ninja。
- **依赖管理**：vcpkg，仓库内置于 `third_party/vcpkg/`。
- **RPC/IDL**：gRPC + Protocol Buffers。
- **配置格式**：YAML。
- **日志**：spdlog。
- **格式化输出**：fmt。
- **测试框架**：GoogleTest + CTest。
- **端到端测试**：C++ E2E client，Python pytest E2E。
- **覆盖率**：gcovr。
- **Benchmark**：内置 `bench_client`，支持 workload、并发、延迟统计、QPS、JSON 输出。
- **核心依赖**：`fmt`、`spdlog`、`protobuf`、`grpc`、`yaml-cpp`、`gtest`。

## 安装指南

### 1. 准备环境

推荐环境：

- macOS 或 Linux。
- C++17 兼容编译器：AppleClang、Clang 或 GCC。
- CMake 3.20 或更高版本。
- Ninja。
- Git。
- Python 3，用于 E2E pytest 和部分脚本。
- gcovr，可选，仅用于 coverage 报告。

macOS 示例：

```bash
brew install cmake ninja python3 gcovr
```

Ubuntu 示例：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build git curl zip unzip tar pkg-config python3 python3-venv
```

### 2. 获取代码

```bash
git clone <repo-url> adviskv
cd adviskv
```

如果 `third_party/vcpkg` 是子模块，请先初始化：

```bash
git submodule update --init --recursive
```

### 3. 初始化依赖

首次拉取代码后，先执行依赖初始化脚本：

```bash
./scripts/setup.sh
```

脚本会执行以下工作：

- 如果 `third_party/vcpkg/vcpkg` 不存在或不可执行，自动 bootstrap vcpkg。
- 通过 vcpkg 安装 `vcpkg.json` 中声明的依赖。
- 将 vcpkg 安装产物放到 `.adviskv_deps/vcpkg/installed/`。
- 将 vcpkg 源码下载缓存和二进制缓存分别放到 `.adviskv_deps/vcpkg/downloads/`、`.adviskv_deps/vcpkg/binary-cache/`。

依赖初始化完成后，日常编译不再主动执行 vcpkg 下载或安装。

### 4. 编译项目

依赖初始化完成后，推荐使用 CMake presets 构建：

```bash
cmake --preset debug
cmake --build --preset debug
```

运行测试：

```bash
ctest --preset debug
```

CMake 会执行以下工作：

- 检查并使用 `.adviskv_deps/vcpkg/installed/` 中已有的 vcpkg 依赖。
- 生成 `proto/*.proto` 对应的 C++ 和 gRPC 代码到当前构建目录的 `generated/` 下。
- 使用 Ninja 编译项目。

如果依赖目录不存在，请先执行：

```bash
./scripts/setup.sh
```

可用 presets：

- `debug`：Debug 构建，输出到 `build/debug/`。
- `release`：Release 构建，输出到 `build/release/`。

也可以继续使用兼容脚本：

```bash
BUILD_TYPE=Debug ./scripts/build.sh
```

脚本默认 `BUILD_TYPE=Release`，输出到 `build/release/`；设置 `BUILD_TYPE=Debug` 时输出到 `build/debug/`。脚本会复用 CMake 中的 proto 生成规则，不再单独手动生成 protobuf/gRPC 代码。

如果希望使用传统 CMake 命令，也可以在执行 `./scripts/setup.sh` 后运行：

```bash
mkdir -p build
cd build
cmake ..
cmake --build . --parallel
```

只构建指定 target：

```bash
cmake --build --preset debug --target meta
```

或使用兼容脚本：

```bash
BUILD_TARGETS="meta sdm storage" ./scripts/build.sh
```

### 5. 编译产物

主要二进制默认位于 `build/release/bin/`；Debug 构建位于 `build/debug/bin/`：

- `build/release/bin/meta`
- `build/release/bin/sdm`
- `build/release/bin/storage`
- `build/release/bin/adviskvctl`
- `build/release/bin/e2e_client`
- `build/release/bin/bench_client`

主要 library target：

- `adviskv_common`
- `adviskv_meta_lib`
- `adviskv_sdm_lib`
- `adviskv_storage_lib`
- `adviskv_sdk`

## 快速开始

### 1. 构建

首次构建前先初始化依赖：

```bash
./scripts/setup.sh
```

之后日常构建只需要执行：

```bash
./scripts/build.sh
```

### 2. 一键启动 Demo Shell

推荐使用 demo 脚本启动交互式 CLI：

```bash
./scripts/adviskvctl_demo.sh
```

该脚本会先停止旧本地进程，清空 `build/` 下的 demo 数据目录，启动默认本地集群，然后进入 `adviskvctl`。退出 shell 或按 `Ctrl-C` 后，脚本会自动关闭本地集群。

### 3. 手动启动本地单节点集群

默认配置文件：

- SDM：`conf/sdm.yaml`，监听 `127.0.0.1:50049`。
- Storage：`conf/storage-1.yaml`，监听 `127.0.0.1:50051`，向 SDM `127.0.0.1:50049` 注册并发送心跳。
- Meta：`conf/meta.yaml`，监听 `127.0.0.1:50048`，连接 SDM `127.0.0.1:50049`。

建议在三个终端中按顺序启动：

```bash
./build/release/bin/sdm --conf=conf/sdm.yaml
```

```bash
./build/release/bin/storage --conf=conf/storage-1.yaml
```

```bash
./build/release/bin/meta --conf=conf/meta.yaml
```

注意：`meta`、`sdm` 和 `storage` 都必须显式使用 `--conf=...` 传入配置文件；相对路径会按项目根目录解析。

### 4. 使用统一 CLI 演示

启动 `adviskvctl` 交互式 shell：

```bash
./build/release/bin/adviskvctl
```

默认读取 `conf/client.yaml`；该文件只描述 CLI/SDK 客户端连接信息和 RPC 超时，不用于启动服务端进程。

在交互式命令行中执行：

```text
create_db demo_db dc1
create_table demo_db demo_table 1 1 default
wait_table demo_db demo_table
put demo_db demo_table k1 v1
get demo_db demo_table k1
route demo_db demo_table k1
get_table demo_db demo_table
quit
```

命令说明：

- `create_db demo_db dc1`：创建数据库 `demo_db`，zone 为 `dc1`。
- `create_table demo_db demo_table 1 1 default`：创建 1 个 shard、1 个 replica 的表，资源池为 `default`。
- `put/get`：通过 SDK 自动向 SDM 查询 route，再访问对应 Storage leader。
- `route demo_db demo_table k1`：展示 key 对应 shard 和 replica leader/follower。
- `get_table demo_db demo_table`：查询表状态。

### 5. 运行基础 E2E

在集群运行状态下执行：

```bash
./build/release/bin/e2e_client --conf=conf/meta.yaml --case=basic_kv --replica_count=1
```

`e2e_client` 参数使用 `--name=value` 格式。常用参数：

- `--case=basic_kv`
- `--meta_host=127.0.0.1`
- `--meta_port=50048`
- `--sdm_host=127.0.0.1`
- `--sdm_port=50049`
- `--db=e2e_db`
- `--table=e2e_table`
- `--shard_count=1`
- `--replica_count=1`
- `--key_count=8`

如果使用默认 `conf/storage-1.yaml` 只启动一个 Storage 节点，建议把 `replica_count` 设为 `1`。默认 benchmark/e2e 配置中部分场景使用 `replica_count=3`，需要准备多个 Storage 节点配置和进程。

### 6. 停止本地进程

```bash
./scripts/stop_cluster.sh
```

该脚本会按进程名停止 `sdm`、`meta` 和 `storage`。

## API 文档与使用说明

### MetaService

协议文件：`proto/meta.proto`。

```protobuf
service MetaService {
  rpc CreateDB(CreateDBRequest) returns (CreateDBResponse);
  rpc CreateTable(CreateTableRequest) returns (CreateTableResponse);
  rpc DropTable(MetaDropTableRequest) returns (MetaDropTableResponse);
  rpc GetTable(GetTableRequest) returns (GetTableResponse);
}
```

接口说明：

- `CreateDB(db_name, zone)`：创建数据库，返回 `db_id`。
- `CreateTable(db_name, table_name, shard_count, replica_count, resource_pool)`：创建表，返回 `table_id`、`table_state` 和 `operation_id`。
- `DropTable(db_name, table_name)`：删除表，返回表状态和 operation id。
- `GetTable(db_name, table_name)`：查询表 ID、shard 数、副本数、表状态和最近错误信息。

典型链路：

```text
Client/CLI -> MetaService.CreateTable -> CatalogManager -> SdmClient.PlaceTable -> SDM
```

### ShardingManagerService

协议文件：`proto/sdm.proto`。

```protobuf
service ShardingManagerService {
  rpc GetRoute(GetRouteRequest) returns (GetRouteResponse);
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc PlaceTable(PlaceTableRequest) returns (PlaceTableResponse);
  rpc DropTable(DropTableRequest) returns (DropTableResponse);
  rpc GetTableStatus(GetTableStatusRequest) returns (GetTableStatusResponse);
  rpc HeartBeat(HeartBeatRequest) returns (HeartBeatResponse);
}
```

接口说明：

- `GetRoute(db_name, table_name, key)`：根据表名和 key 返回 `table_id`、`shard_id` 和 replica endpoint 列表。
- `RegisterNode(node_id, ip, port, resource_pool, dc)`：注册 Storage 节点。
- `PlaceTable(...)`：接收 Meta 发起的建表部署请求。
- `DropTable(table_id, operation_id)`：接收 Meta 发起的删表请求。
- `GetTableStatus(operation_id, table_id)`：查询 SDM 侧表部署状态。
- `HeartBeat(...)`：Storage NodeAgent 周期上报节点和 replica 状态。

SDM 内部关键组件：

- `SdmStore`：控制面元数据门面。
- `SdmPersistEngine`：SDM 本地持久化引擎。
- `DefaultNodeSelector`：根据资源池和节点状态选择 replica 放置节点。
- `TableReconciler`：推进 table desired state 到实际状态。
- `RouteUpdateCheckTask`：根据 READY replica 生成 route。
- `HeartBeatCheckTask`：维护节点在线、疑似、离线状态。

### StorageService

协议文件：`proto/storage.proto`。

```protobuf
service StorageService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc CreateReplica(CreateReplicaRequest) returns (CreateReplicaResponse);
  rpc DeleteReplica(DeleteReplicaRequest) returns (DeleteReplicaResponse);
  rpc GetReplicaInfo(GetReplicaInfoRequest) returns (GetReplicaInfoResponse);
  rpc RequestVote(RequestVoteRequest) returns(RequestVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns(AppendEntriesResponse);
  rpc InstallSnapshot(InstallSnapshotRequest) returns(InstallSnapshotResponse);
  rpc TestGetReplicaState(TestGetReplicaStateRequest) returns (TestGetReplicaStateResponse);
}
```

接口说明：

- `Put(table_id, shard_id, key, value)`：写入 KV 数据。
- `Get(table_id, shard_id, key)`：读取 KV 数据。
- `Delete(table_id, shard_id, key)`：删除 KV 数据。
- `CreateReplica(table_id, shard_index, replica_index, engine_type, members)`：创建本地 replica。
- `DeleteReplica(table_id, shard_id, replica_id)`：删除本地 replica。
- `GetReplicaInfo(table_id, shard_id, replica_id)`：查询本地 replica 信息。
- `RequestVote`、`AppendEntries`、`InstallSnapshot`：Raft 内部 RPC。
- `TestGetReplicaState`：测试辅助接口，用于查询 replica 当前 Raft 状态。

Storage 内部关键组件：

- `ReplicaManager`：管理本地所有 replica，负责恢复、创建、删除和 tick 调度。
- `Replica`：封装 RaftNode、状态机、持久化和发送器。
- `RaftNode`：实现 Raft 角色转换、投票、日志复制、commit、apply、snapshot 等逻辑。
- `RaftSender` / `GrpcRaftRpcTransport`：发送 Raft RPC。
- `KVStateMachine`：把已提交日志应用到 KV engine。
- `PersistEngine`：WAL 和 snapshot 持久化。
- `ReplicaMetaPersistEngine`：replica 元信息持久化。
- `NodeAgent`：向 SDM 注册节点并周期发送 heartbeat。

### C++ SDK

核心头文件：`src/sdk/client.h`。

```cpp
#include <iostream>
#include "sdk/client.h"

int main() {
    adviskv::sdk::KVClientConf conf;
    conf.db_name = "demo_db";
    conf.table_name = "demo_table";
    conf.sdm_host = "127.0.0.1";
    conf.sdm_port = 50049;
    conf.sdm_timeout_ms = 2000;
    conf.storage_timeout_ms = 3000;

    adviskv::sdk::KVClient client(conf);

    adviskv::Status put_status = client.put("hello", "world");
    if (put_status.fail()) {
        std::cerr << put_status.to_string() << std::endl;
        return 1;
    }

    adviskv::Value value;
    adviskv::Status get_status = client.get("hello", &value);
    if (get_status.ok()) {
        std::cout << value << std::endl;
    }

    return get_status.ok() ? 0 : 1;
}
```

SDK 行为：

- 每次请求先通过 SDM `GetRoute` 获取目标 `table_id`、`shard_id` 和 replica endpoint。
- Storage 返回 `NOT_LEADER`、`REPLICA_NOT_FOUND` 或 `ROUTE_NOT_FOUND` 时，SDK 会重新解析 route 并重试一次。
- `NOT_YET_COMMIT` 会被转换为 `UNKNOWN`，调用方应稍后重试或通过后续 `get` 确认结果。

### CLI 工具

#### adviskvctl

`adviskvctl` 是推荐的统一演示入口，启动后进入交互式 shell。

```bash
./build/release/bin/adviskvctl
```

默认读取 `conf/client.yaml`；如需连接另一套集群，可使用 `--conf=<client.yaml>` 指定客户端配置。

Shell 内常用命令：

```text
create_db <db> <zone>
create_table <db> <table> <shards> <replicas> <resource_pool>
wait_table <db> <table> [timeout_ms]
put <db> <table> <key> <value>
get <db> <table> <key>
delete <db> <table> <key>
route <db> <table> <key>
demo status
demo kill <service>
demo restart <service>
```

`demo *` 命令只用于本地演示环境，会桥接 `scripts/local_cluster.py` 管理本机进程，不代表线上数据库管理接口。

## 配置说明

### Meta 配置

示例文件：`conf/meta.yaml`，启动时必须通过 `--conf=conf/meta.yaml` 显式指定。

- `listen_host`：Meta 监听地址，默认 `127.0.0.1`。
- `port`：Meta 监听端口，当前默认 `50048`。
- `sdm_host` / `sdm_port`：Meta 连接的 SDM 地址，当前默认 `127.0.0.1:50049`。
- `data_dir`：Meta catalog 持久化目录。
- `log_dir` / `log_filename` / `log_level`：日志配置。

### SDM 配置

示例文件：`conf/sdm.yaml`，启动时必须通过 `--conf=conf/sdm.yaml` 显式指定。

- `listen_host`：SDM 监听地址，默认 `127.0.0.1`。
- `port`：SDM 监听端口，当前默认 `50049`。
- `data_dir`：SDM metastore 持久化目录。
- `log_dir` / `log_filename` / `log_level`：日志配置。

### Storage 配置

示例文件：`conf/storage-1.yaml`，启动时必须通过 `--conf=conf/storage-1.yaml` 显式指定。

- `node_id`：Storage 节点 ID，例如 `storage-1`。
- `ip` / `port`：Storage 对外服务地址，当前默认 `127.0.0.1:50051`。
- `listen_host`：gRPC 监听地址。
- `resource_pool`：节点所属资源池，默认 `default`。
- `dc`：节点所属机房/区域标识。
- `data_dir`：Storage 本地数据目录。
- `heartbeat_interval_ms`：向 SDM 发送心跳的间隔。
- `raft_rpc_timeout_ms`：Raft RPC 超时时间。
- `manager_host` / `manager_port`：SDM 地址。
- `metrics_http_enable`：是否启用 metrics HTTP server。
- `metrics_http_host` / `metrics_http_port` / `metrics_http_path`：metrics HTTP 配置，默认 `127.0.0.1:51051/metrics`。

### 本地运行目录

默认本地运行态文件统一放在 `build/runtime/`：

```text
build/runtime/
  data/
    meta/
    sdm/
    storage-1/
    storage-2/
    storage-3/
  logs/
    meta/
    sdm/
    storage-1/
    storage-2/
    storage-3/
```

- `data/`：Meta、SDM、Storage 的持久化数据。
- `logs/`：服务内部日志；`local_cluster.py` 捕获的进程标准流分别写入对应服务目录下的 `stdout.log` 和 `stderr.log`。
- `scripts/adviskvctl_demo.sh` 会在启动前清空 `build/runtime/`，确保 demo 从干净集群开始。

## 项目结构

```text
adviskv/
├── CMakeLists.txt             # 顶层 CMake 配置
├── README.md                  # 项目说明文档
├── vcpkg.json                 # vcpkg 依赖声明
├── conf/                      # 本地默认配置
│   ├── meta.yaml
│   ├── sdm.yaml
│   └── storage-1.yaml
├── proto/                     # protobuf/gRPC 协议定义
│   ├── common.proto
│   ├── meta.proto
│   ├── sdm.proto
│   └── storage.proto
├── scripts/                   # 构建、测试、覆盖率、benchmark、进程管理脚本
│   ├── setup.sh
│   ├── build.sh
│   ├── run_test.sh
│   ├── e2e_pytest.sh
│   ├── coverage.sh
│   ├── bench.sh
│   ├── adviskvctl_demo.sh
│   ├── local_cluster.py
│   └── stop_cluster.sh
├── src/
│   ├── common/                # 日志、配置、状态码、metrics、线程池、后台任务等通用组件
│   ├── meta/                  # DDL、catalog、Meta 持久化、Meta gRPC 服务
│   ├── sdm/                   # 控制面：node/table/route/heartbeat/reconciler/metastore
│   ├── storage/               # 数据面：Raft、replica、engine、persist、NodeAgent、Storage RPC
│   └── sdk/                   # C++ KVClient、route client、storage client
├── tools/
│   ├── cli/                   # adviskvctl 统一交互式 CLI
│   ├── e2e/                   # C++ E2E client 和场景工具
│   └── bench/                 # benchmark client
├── test/                      # GoogleTest 单测和 Python E2E 测试
│   ├── common/
│   ├── meta/
│   ├── sdm/
│   ├── storage/
│   └── e2e/
├── docs/                      # 设计文档、开发日志和阶段性记录
└── third_party/vcpkg/         # vcpkg 依赖管理工具
```

## 测试

### 单元测试

```bash
./scripts/run_test.sh
```

该脚本会先执行 `./scripts/build.sh`，然后运行：

```bash
ctest --test-dir build --output-on-failure
```

如果是首次运行或依赖发生变化，请先执行 `./scripts/setup.sh`。

### 覆盖率

```bash
./scripts/coverage.sh
```

输出文件：

- `build-coverage/coverage.html`
- `build-coverage/coverage-report/index.html`
- `build-coverage/coverage-report/coverage.xml`

### Python E2E

```bash
./scripts/e2e_pytest.sh
```

脚本行为：

- 创建或复用 `.venv`。
- 安装 `test/e2e/requirements.txt` 中的依赖。
- 调用 `scripts/stop_cluster.sh` 清理旧进程。
- 通过 pytest 执行 `test/e2e` 下的端到端测试。

## Benchmark 测试结果

## Benchmark 工具

项目提供 `bench_client` 和 `scripts/bench.sh` 用于压测。`Benchmark 测试结果` 章节预留给正式测试数据，本节只说明工具用法。

构建：

```bash
./scripts/build.sh
```

运行示例：

```bash
./build/release/bin/bench_client \
  --meta_host=127.0.0.1 \
  --meta_port=50048 \
  --sdm_host=127.0.0.1 \
  --sdm_port=50049 \
  --db=bench_db \
  --table=bench_table \
  --zone=dc1 \
  --resource_pool=default \
  --shard_count=1 \
  --replica_count=1 \
  --workload=put \
  --threads=1 \
  --requests=1000 \
  --key_count=1000 \
  --value_size=128 \
  --output_json=build/bench-result.json
```

支持 workload：

- `put`：纯写入。
- `get`：纯读取，运行前会准备可读数据集。
- `mixed`：混合读写，通过 `--read_ratio` 控制读比例。

常用参数：

- `--threads=<n>`：并发线程数。
- `--requests=<n>`：正式压测请求数。
- `--warmup_requests=<n>`：预热请求数。
- `--key_count=<n>`：key 空间大小。
- `--value_size=<bytes>`：value 大小。
- `--output_json=<path>`：将结果写入 JSON。
- `--metrics_http_port=<port>`：为 benchmark 进程暴露 metrics HTTP server。
- `--metrics_hold_seconds=<n>`：压测结束后保留进程一段时间，方便抓取 metrics。

也可以通过脚本入口运行：

```bash
./scripts/bench.sh --workload=put --requests=1000 --replica_count=5
```

`scripts/bench.sh` 会启动 `conf/storage-1.yaml` 到 `conf/storage-5.yaml` 共 5 个 Storage 节点。`--replica_count=5` 表示每个 shard 放 5 个副本；如果只想启动 5 个节点但每个 shard 放 3 个副本，可以保留默认 `--replica_count=3`。

## 开发状态与限制

当前已经具备的主干能力：

- `Meta -> SDM -> Storage` 建表主链路。
- Storage 节点注册、心跳和 replica 状态上报。
- SDM placement、table reconcile、route update。
- SDK route 解析和 KV 读写。
- Storage Raft、WAL、snapshot、replica 恢复相关实现。
- 单测、E2E、coverage、benchmark 工具链。

当前仍然需要谨慎看待的限制：

- 默认配置只提供一个 Storage 节点；多副本实验需要额外准备多个 Storage 配置和进程。
- 控制面目前不是高可用部署，Meta 和 SDM 均为单进程本地模式。
- 尚未实现 Raft config change、自动扩缩容、自动 rebalance。
- route 和 leader 可写路由语义仍需要在更多故障场景下验证。
- 当前 Storage 使用 map-based KV engine，还未集成 RocksDB 等生产级 LSM 存储引擎。
- Benchmark 结果尚未正式沉淀，性能结论应以后续实测数据为准。

## 许可证信息

本项目采用 MIT License 开源，详见 [LICENSE](LICENSE)。

已知第三方信息：

- `third_party/vcpkg/LICENSE.txt` 是 vcpkg 自身的 MIT License。
- 项目通过 vcpkg 使用 `fmt`、`spdlog`、`protobuf`、`grpc`、`yaml-cpp`、`gtest` 等依赖；这些依赖的许可证应在正式发布前单独核查并在文档中列明。
