#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/define.h"

#include "common/id_allocator.h"
#include "common/model/model.h"
#include "common/optional.h"
#include "common/shard_range.h"

namespace adviskv::meta {

enum class TablePhase : int32_t {
    CREATING = 0,
    READY = 1,
    DELETING = 2,
    DELETED = 3,
    RESIZING = 4,

    MAX_VALUE = RESIZING,
};

// Table/Replica 的 desired 已并入 phase：DELETING/DELETED 即 ABSENT。
inline bool is_absent(TablePhase phase) {
    return phase == TablePhase::DELETING || phase == TablePhase::DELETED;
}

enum class ReplicaPhase : int32_t {
    CREATING = 0,
    READY = 1,
    LOST = 2,
    ERROR = 3,
    DELETING = 4,
    DELETED = 5,

    MAX_VALUE = DELETED,
};

inline bool is_absent(ReplicaPhase phase) {
    return phase == ReplicaPhase::DELETING || phase == ReplicaPhase::DELETED;
}

struct ReplicaSpec {
    std::string dc;
    NodeID assign_node_id{""};
    EngineType engine_type{EngineType::MAP};
};

struct ReplicaState {
    ReplicaPhase phase{ReplicaPhase::CREATING};
    int64 update_ts{0};
};

// Storage 心跳上报的观测态，不落盘。
struct ReplicaObserved {
    ReplicaRole raft_role{ReplicaRole::FOLLOWER};
    RaftMemberType member_type{RaftMemberType::NON_MEMBER};
    Endpoint endpoint;
    StorageReplicaStatus storage_status{StorageReplicaStatus::INITIALIZING};
    bool no_exist{false};
    int64 update_ts{0};
    Term term{0};
};

struct Replica {
    ReplicaID replica_id;
    ReplicaSpec spec;
    ReplicaState state;
};

// shard 的 leader membership 观测（谁在上报、上报时的 term），心跳重建，不落盘。
// 防抖：更高 term 可覆盖；同 term 只接受同一个 leader 重复上报。
struct MembershipObserved {
    Term term{0};
    ReplicaID leader;
};

using ReplicaOr = Optional<Replica>;
using ReplicaObservedOr = Optional<ReplicaObserved>;
using MembershipObservedOr = Optional<MembershipObserved>;

using ReplicaPtr = std::shared_ptr<Replica>;

enum class ReplicaGroupMode : int32_t {
    BOOTSTRAP = 0,
    RAFT_RECONFIG = 1,

    MAX_VALUE = RAFT_RECONFIG,
};

// 只描述"应该是什么"，落盘。leader/term 是观测，放 RuntimeData 的
// MembershipObserved，不放这里（不然每轮稳态心跳都会重写 meta.json）。
struct ReplicaGroup {
    ShardID shard_id;
    ReplicaGroupMode mode{ReplicaGroupMode::BOOTSTRAP};
    int32_t target_replica_count{0};
    std::vector<ReplicaID> desired_members;
    IDAllocator<ReplicaSeq> seq_allocator;
};

using ReplicaGroupPtr = std::shared_ptr<ReplicaGroup>;

enum class NodeStatus : int32_t {
    ONLINE = 0,
    SUSPECT = 1,
    OFFLINE = 2,

    MAX_VALUE = OFFLINE,
};

enum class NodeOfflineReason : int32_t {
    NONE = 0,
    TOPOLOGY_MISMATCH = 1,

    MAX_VALUE = TOPOLOGY_MISMATCH,
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

using ReplicaGroupOr = Optional<ReplicaGroup>;
using NodeOr = Optional<Node>;
using DatabaseOr = Optional<Database>;
using TableOr = Optional<Table>;
using ShardRouteOr = Optional<ShardRoute>;

}  // namespace adviskv::meta
