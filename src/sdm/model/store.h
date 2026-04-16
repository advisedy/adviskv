#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/type.h"
#include "meta.pb.h"
#include "meta/service/ddl_service.h"

namespace adviskv::sdm {

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
};

struct ReplicaState {
    NodeID assign_node_id{""};
    Endpoint endpoint;
    ReplicaStatus status{ReplicaStatus::PENDING};
    ReplicaRole role;
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
};

struct NodeState {
    Endpoint endpoint;
    int32_t owned_replica_count{0};
    int32_t leader_count{0};
    int64_t last_heartbeat_ts{0};
    NodeStatus status;
};

struct Node {
    NodeID id;
    NodeSpec spec;
    NodeState state;
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

}  // namespace adviskv::sdm