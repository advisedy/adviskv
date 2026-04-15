#pragma once
#include "common/type.h"
#include "meta.pb.h"
#include "meta/service/ddl_service.h"
#include <cstdint>
#include <string>


namespace adviskv::sdm{

using PlaceTableParam = rpc::PlaceTableRequest;

//////////////////////////////
// node


struct Endpoint{
    std::string ip;
    int32_t port;
};

struct NodeSpec{
    std::string resource_pool;
    std::string dc;
};

struct NodeState{
    Endpoint endpoint;
    int32_t owned_replica_count{0};
    int32_t leader_count{0};
    int64_t last_heartbeat_ts{0};
};

struct Node{
    NodeID id;
    NodeSpec spec;
    NodeState state;
};

//////////////////////////////
// table

enum class TableStatus{
    CREATEING = 1,
};

struct TableSpec{
    std::string table_name;
    DatabaseID db_id;
    std::string db_name;
    int32_t shard_count;
    int32_t replica_count;
    std::string resource_pool;
};

struct TableState{
    TableStatus status;
};

struct Table{
    TableID table_id;
    TableSpec spec;
    TableState state;
};

//////////////////////////////
// replica

enum class ReplicaStatus {
    PENDING = 1,
    ADDING = 2,
    READY = 3,
    ERROR = 4,
};

struct ReplicaKey{
    TableID table_id;
    ShardID shard_id;
    int32_t replica_index;
};

struct ReplicaSpec{
    std::string dc;
};

struct ReplicaState{
    NodeID assign_node_id;
    Endpoint endpoint;
    ReplicaStatus status{ReplicaStatus::PENDING};
};

struct Replica{
    ReplicaKey replica_key;
    ReplicaSpec spec;
    ReplicaState state;
};

//////////////////////////////
// shard_route

struct RouteEntry {
    ReplicaKey replica_key;
    NodeID node_id;
    std::string ip;
    int32_t port{0};
};

struct ShardRoute {
    TableID table_id{-1};
    ShardID shard_id{-1};
    std::vector<RouteEntry> replicas;
};

}