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

struct Endpoint {
    std::string ip;
    int32_t port;
};

//////////////////////////////
// resource_pool

struct ResourcePool {
    std::string name;
    std::vector<NodeID> nodes;
    std::vector<TableID> tables;
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

struct ReplicaKey {
    TableID table_id;
    ShardID shard_id;
    int32_t replica_index;
};

struct ReplicaSpec {
    std::string dc;
    NodeID assign_node_id{""};
    ReplicaRole role; // 期望的role ， 目前sdm这边记录的role
    ReplicaStatus status;  // 目前sdm这边记录的status
    Endpoint endpoint;  // 目前smd这边记录的endpoint
};

struct ReplicaState {
    Endpoint endpoint;
    ReplicaStatus status{ReplicaStatus::ERROR};
    ReplicaRole role; // 实际返回的role
};

struct Replica {
    ReplicaKey replica_key;
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

struct NodeSpec {
    std::string resource_pool;
    std::string dc;
    Endpoint endpoint; // 这边sdm实际记录的endpoint
    NodeStatus status; // 这边sdm 实际记录的status

};

struct NodeState {
    Endpoint endpoint;
    // int32_t owned_replica_count{0};
    // int32_t leader_count{0};
    int64_t last_heartbeat_ts{0};
    NodeStatus status;
};

struct NodeDerived {
    int32_t owned_replica_count{0};
    int32_t owned_leader_count{0};
    // bool schedulable{false};
};

struct Node {
    NodeID id;
    NodeSpec spec;
    NodeState state;
    NodeDerived derived;
    std::vector<ReplicaKey> replicas;
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
    ReplicaKey replica_key;
    NodeID node_id;
    std::string sp;
    int32_t port{0};
};

struct ShardRoute {
    TableID table_id{-1};
    ShardID shard_id{-1};
    std::vector<RouteEntry> replicas;
};

using ShardRoutePtr = std::shared_ptr<ShardRoute>;

///////////////// 内置pb对他们的转换

}  // namespace adviskv::sdm