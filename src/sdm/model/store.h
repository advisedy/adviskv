#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/define.h"
#include "common/optional.h"
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

enum class ReplicaDesired {
    PRESENT = 1,
    ABSENT = 2,
};

enum class ReplicaPhase {
    PENDING = 1,
    CREATING = 2,
    READY = 3,
    DELETING = 4,
    DELETED = 5,
    LOST = 6,
    ERROR = 7,
};

struct ReplicaSpec {
    std::string dc;
    NodeID assign_node_id{""};
    EngineType engine_type{EngineType::MAP};
    std::vector<PeerMember> members;
};
// 这边replca的status，RPC发送的时候会有，我们在心跳服务里面处理了， 这边就
// 不会再存起来了。
struct ReplicaState {
    ReplicaDesired desired{ReplicaDesired::PRESENT};
    ReplicaPhase phase{ReplicaPhase::PENDING};
    ReplicaRole observed_role{ReplicaRole::FOLLOWER};
    Endpoint observed_endpoint;
    std::string last_error_msg;
    int64 update_ts{0};
    Term term{0};
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

enum class TableDesired : int32 {
    PRESENT = 1,
    ABSENT = 2,
};

enum class TablePhase : int32 {
    CREATING = 1,
    READY = 2,
    DELETING = 3,
    DELETED = 4,
    FAILED = 5,
};

struct TableSpec {
    std::string table_name;
    DatabaseID db_id;
    std::string db_name;
    int32_t shard_count;
    int32_t replica_count;
    std::string resource_pool;
    std::string operation_id;
};

struct TableState {
    TableDesired desired{TableDesired::PRESENT};
    TablePhase phase{TablePhase::CREATING};
    std::string last_error_msg;
    int64_t update_ts{0};
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
    std::string ip;
    int32_t port{0};
    ReplicaRole role{ReplicaRole::FOLLOWER};
    Term term{0};
};

struct ShardRoute {
    ShardID shard_id;
    std::vector<RouteEntry> replicas;
};

using ShardRoutePtr = std::shared_ptr<ShardRoute>;

///////////////// 内置pb对他们的转换

// #define USING_TYPE_OR(name) using name##Or = std::optional<name>;

// USING_TYPE_OR(ResourcePool)

// template <typename T>
// class Optional : public std::optional<T> {
//    public:
//     bool empty() { return !this->has_value(); }
//     T* point() {
//         if (this->has_value()) return nullptr;
//         return &this->value();
//     }

//     std::optional<T> self() { return *this; }

//     bool operator==(const Optional<T>& other) const {
//         return self() == other.self();
//     }

//     DEFINE_OPERATOR_NOT_EQUAL(Optional<T>)
// };

using ResourcePoolOr = Optional<ResourcePool>;
using ReplicaOr = Optional<Replica>;
using NodeOr = Optional<Node>;
using TableOr = Optional<Table>;
using ShardRouteOr = Optional<ShardRoute>;

}  // namespace adviskv::sdm
