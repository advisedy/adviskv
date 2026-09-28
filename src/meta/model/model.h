#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/define.h"
#include "common/id_allocator.h"
#include "common/model/type.h"
#include "common/optional.h"
#include "common/shard_range.h"

namespace adviskv::meta {

enum class TableDesired : int8 {
    PRESENT = 1,
    ABSENT = 2,
};

enum class TablePhase : int8 {
    CREATING = 1,
    READY = 2,
    DELETING = 3,
    DELETED = 4,
    FAILED = 5,
    RESIZING = 6,
};

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
    ERROR = 7,
};

struct ReplicaSpec {
    std::string dc;
    NodeID assign_node_id{""};
    EngineType engine_type{EngineType::MAP};
};

struct ReplicaState {
    ReplicaDesired desired{ReplicaDesired::PRESENT};
    ReplicaPhase phase{ReplicaPhase::PENDING};
    ReplicaRole observed_raft_role{ReplicaRole::FOLLOWER};
    RaftMemberType observed_member_type{RaftMemberType::NON_MEMBER};
    Endpoint observed_endpoint;
    StorageReplicaStatus observed_storage_status{StorageReplicaStatus::INITIALIZING};
    bool observed_no_exist{false};
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

enum class ReplicaGroupMode : int32 {
    BOOTSTRAP = 1,
    RAFT_RECONFIG = 2,
};

struct ReplicaGroup {
    ShardID shard_id;
    ReplicaGroupMode mode{ReplicaGroupMode::BOOTSTRAP};
    int32_t target_replica_count{0};
    std::vector<ReplicaID> desired_members;
    Term observed_membership_term{0};
    ReplicaID observed_membership_leader;
    IDAllocator<ReplicaSeq> seq_allocator;
};

using ReplicaGroupPtr = std::shared_ptr<ReplicaGroup>;

enum class NodeStatus : int8 {
    ONLINE = 1,
    SUSPECT = 2,
    OFFLINE = 3,
};

enum class NodeOfflineReason : int8 {
    NONE = 0,
    TOPOLOGY_MISMATCH = 1,
};

struct NodeMeta {
    std::string resource_pool;
    std::string dc;
};

struct NodeState {
    NodeStatus status{NodeStatus::ONLINE};
    Endpoint endpoint;
    int64_t last_heartbeat_ts{0};
    NodeOfflineReason offline_reason{NodeOfflineReason::NONE};
};

struct Node {
    NodeID id;
    NodeMeta meta;
    NodeState state;
};

using NodePtr = std::shared_ptr<Node>;

struct Database {
    DatabaseID db_id{-1};
    std::string name;
    std::string zone;
};

using DatabasePtr = std::shared_ptr<Database>;

struct TableSpec {
    DatabaseID db_id{-1};
    std::string db_name;
    std::string name;
    EngineType engine_type{EngineType::MAP};
    std::vector<ShardRange> shard_ranges{};
    int32_t target_replica_count{0};
    std::string resource_pool;
};

struct TableState {
    TableDesired desired{TableDesired::PRESENT};
    TablePhase phase{TablePhase::CREATING};
    std::string operation_id;
    std::string last_error_msg;
    int64_t create_ts{0};
    int64_t update_ts{0};
};

struct Table {
    TableID table_id{-1};
    TableSpec spec;
    TableState state;

    int32_t shard_count() const { return static_cast<int32_t>(spec.shard_ranges.size()); }
};

using TablePtr = std::shared_ptr<Table>;

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
    HashRange hash_range{};
};

using ShardRoutePtr = std::shared_ptr<ShardRoute>;

using ReplicaOr = Optional<Replica>;
using ReplicaGroupOr = Optional<ReplicaGroup>;
using NodeOr = Optional<Node>;
using DatabaseOr = Optional<Database>;
using TableOr = Optional<Table>;
using ShardRouteOr = Optional<ShardRoute>;

}  // namespace adviskv::meta
