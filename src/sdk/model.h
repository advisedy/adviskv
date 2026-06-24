#pragma once

#include <cstdint>
#include <vector>

#include "common/type.h"

namespace adviskv::sdk {

struct RouteReplica {
    ReplicaID replica_id;
    Endpoint endpoint;
    ReplicaRole role{ReplicaRole::FOLLOWER};
};

struct RouteInfo {
    TableID table_id{-1};
    ShardIndex shard_id{-1};
    std::vector<RouteReplica> replicas;
};
}  // namespace adviskv::sdk