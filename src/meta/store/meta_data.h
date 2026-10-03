#pragma once

#include <string>
#include <unordered_map>

#include "common/id_allocator.h"
#include "common/model/model.h"
#include "meta/model/model.h"

namespace adviskv::meta {

struct DurableData {
    IDAllocator<DatabaseID> db_id_allocator;
    IDAllocator<TableID> table_id_allocator;
    std::unordered_map<DatabaseID, Database> databases;
    std::unordered_map<TableID, Table> tables;
    std::unordered_map<ReplicaID, Replica, ReplicaIDHash> replicas;
    std::unordered_map<ShardID, ReplicaGroup, ShardIDHash> replica_groups;
};

struct RuntimeData {
    std::unordered_map<NodeID, Node> nodes;
    std::unordered_map<ShardID, ShardRoute, ShardIDHash> shard_routes;
    // 心跳观测态，重启后重新上报。
    std::unordered_map<ReplicaID, ReplicaObserved, ReplicaIDHash> replica_observeds;
    std::unordered_map<ShardID, MembershipObserved, ShardIDHash> membership_observeds;
};

}  // namespace adviskv::meta
