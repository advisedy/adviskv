#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/type.h"

namespace adviskv::sdm {

/*
对于Spec的定义: 控制面定义/相对稳定/desired
对于State的定义: 运行时观测/heartbeat 上报/observed

*/

// using PlaceTableParam = rpc::PlaceTableRequest;



//////////////////////////////
// resource_pool

struct ResourcePool {
    std::string name;
    // std::vector<NodeID> nodes;
    // std::vector<TableID> tables;
};
using ResourcePoolPtr = std::shared_ptr<ResourcePool>;

//////////////////////////////
// replica

enum class ReplicaStatus {
    PENDING = 1,
    ADDING = 2,
    READY = 3,
    LOST = 4,
    ERROR = 5,
};

enum class ReplicaRole {
    LEADER = 1,
    FOLLOWER = 2,
};

struct ReplicaSpec {
    std::string dc;
    NodeID assign_node_id{""};
    ReplicaRole role;      // 目前sdm这边记录的role
    ReplicaStatus status;  // 目前sdm这边记录的status
};
// 这边replca的status，RPC发送的时候会有，我们在心跳服务里面处理了， 这边就
// 不会再存起来了。
struct ReplicaState {
    Endpoint endpoint;
    ReplicaRole role;  // 实际返回的role
};

struct Replica {
    ReplicaID replica_id;
    ReplicaSpec spec;
    ReplicaState state;
};

using ReplicaPtr = std::shared_ptr<Replica>;

//////////////////////////////
// node

enum class NodeStatus {
    ONLINE = 1,
    OFFLINE = 2,
    SUSPECT = 3,
};
// 注意，node那边只会发送过来心跳时间， 状态是sdm决定的
struct NodeSpec {
    std::string resource_pool;
    std::string dc;
    NodeStatus status;
};

struct NodeState {
    Endpoint endpoint;
    int64_t last_heartbeat_ts{0};
};

struct NodeDerived {
    int32_t owned_replica_count{0};
    int32_t owned_leader_count{0};
};

struct Node {
    NodeID id;
    NodeSpec spec;
    NodeState state;
    NodeDerived derived;
};

using NodePtr = std::shared_ptr<Node>;
//////////////////////////////
// table

enum class TableStatus {
    CREATEING = 1,
};

struct TableSpec {
    std::string table_name;
    DatabaseID db_id;
    std::string db_name;
    int32_t shard_count;
    int32_t replica_count;
    std::string resource_pool;
};

struct TableState {
    TableStatus status;
};

struct Table {
    TableID table_id;
    TableSpec spec;
    TableState state;
};

using TablePtr = std::shared_ptr<Table>;

//////////////////////////////
// shard_route

struct RouteEntry {
    ReplicaID replica_id;
    NodeID node_id;
    std::string sp;
    int32_t port{0};
};

struct ShardRoute {
    ShardID shard_id;
    std::vector<RouteEntry> replicas;
};

using ShardRoutePtr = std::shared_ptr<ShardRoute>;

///////////////// 内置pb对他们的转换

}  // namespace adviskv::sdm
