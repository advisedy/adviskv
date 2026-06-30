#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/define.h"
#include "common/id_allocator.h"
#include "common/model/raft_member_type.h"
#include "common/model/storage_replica_status.h"
#include "common/optional.h"
#include "common/type.h"
#include "sdm/model/table_status.h"

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
};
using ResourcePoolPtr = std::shared_ptr<ResourcePool>;

enum class ReplicaDesired : int8 {
    PRESENT = 1,
    ABSENT = 2,
};

enum class ReplicaPhase : int8 {
    PENDING = 1,
    CREATING = 2,
    READY = 3,
    DELETING = 4,
    DELETED = 5,
    LOST = 6,
    ERROR = 7,  // TODO111 之后应该也会多个阶段
};

struct ReplicaSpec {
    std::string dc;             // 虽然会修改，但是需要持久化
    NodeID assign_node_id{""};  // 虽然会修改，但是需要持久化
    EngineType engine_type{EngineType::MAP};
};

struct ReplicaState {
    ReplicaDesired desired{ReplicaDesired::PRESENT};                                   // ReplicaGroupService
    ReplicaPhase phase{ReplicaPhase::PENDING};                                         // ReplicaGroupService
    ReplicaRole observed_raft_role{ReplicaRole::FOLLOWER};                             // NodeService
    RaftMemberType observed_member_type{RaftMemberType::NON_MEMBER};                   // NodeService
    Endpoint observed_endpoint;                                                        // NodeService
    StorageReplicaStatus observed_storage_status{StorageReplicaStatus::INITIALIZING};  // NodeService //TODO111
    bool observed_no_exist{false};                                                     // NodeService
    std::string last_error_msg;                                                        // ReplicaGroupService
    int64 update_ts{0};                                                                // evertone
    Term term{0};                                                                      // NodeService
};

struct Replica {
    ReplicaID replica_id;
    ReplicaSpec spec;
    ReplicaState state;
};

using ReplicaPtr = std::shared_ptr<Replica>;

enum class ReplicaGroupMode : int32 {
    BOOTSTRAP = 1,
    RAFT_RECONFIG = 2,
};

struct ReplicaGroup {
    ShardID shard_id;
    ReplicaGroupMode mode{ReplicaGroupMode::BOOTSTRAP};  // ReplicaGroupService
    int32_t target_replica_count{0};                     // ReplicaGroupService
    std::vector<ReplicaID> desired_members;              // ReplicaGroupService
    IDAllocator<ReplicaSeq> seq_allocator;               // ReplicaGroupService
};

using ReplicaGroupPtr = std::shared_ptr<ReplicaGroup>;

//////////////////////////////
// node

enum class NodeStatus : int8 {
    ONLINE = 1,
    SUSPECT = 2,
    OFFLINE = 3,
};
struct NodeMeta {
    std::string resource_pool;  // NodeService
    std::string dc;             // NodeService
};

struct NodeState {
    NodeStatus status{NodeStatus::ONLINE};  // NodeService
    Endpoint endpoint;                      // NodeService
    int64_t last_heartbeat_ts{0};           // NodeService
};

struct NodeDerived {
    int32_t owned_replica_count{0};
    int32_t owned_leader_count{0};
};

struct Node {
    NodeID id;
    NodeMeta meta;
    NodeState state;
    NodeDerived derived;
};

using NodePtr = std::shared_ptr<Node>;
//////////////////////////////
// table

// 创建的时候就定下来的：// TableService
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
    TableDesired desired{TableDesired::PRESENT};  // TableService
    TablePhase phase{TablePhase::CREATING};       // TableService
    std::string last_error_msg;                   // TableService
    int64_t update_ts{0};                         // everyone
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
    std::vector<RouteEntry> replicas;  // RouteService
};

using ShardRoutePtr = std::shared_ptr<ShardRoute>;

///////////////// 内置pb对他们的转换

using ResourcePoolOr = Optional<ResourcePool>;
using ReplicaOr = Optional<Replica>;
using ReplicaGroupOr = Optional<ReplicaGroup>;
using NodeOr = Optional<Node>;
using TableOr = Optional<Table>;
using ShardRouteOr = Optional<ShardRoute>;

}  // namespace adviskv::sdm
