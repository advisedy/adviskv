#pragma once

#include <string>
#include <unordered_map>

#include "common/id_allocator.h"
#include "common/model/type.h"
#include "meta/model/model.h"
#include "meta/store/meta_index.h"

namespace adviskv::meta {

struct DurableData {
    IDAllocator<DatabaseID> db_id_allocator;
    IDAllocator<TableID> table_id_allocator;
    std::unordered_map<DatabaseID, Database> databases;
    std::unordered_map<TableID, Table> tables;
    std::unordered_map<ReplicaID, Replica, ReplicaIDHash> replicas;
    std::unordered_map<std::string, ResourcePool> resource_pools;
    std::unordered_map<ShardID, ReplicaGroup, ShardIDHash> replica_groups;
};

struct RuntimeData {
    std::unordered_map<NodeID, Node> nodes;
    std::unordered_map<ShardID, ShardRoute, ShardIDHash> shard_routes;
};

struct MetaWorkingSet {
    DurableData durable;
    RuntimeData runtime;
    MetaIndex index;
};

}  // namespace adviskv::meta
