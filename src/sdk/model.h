#pragma once

#include <cstdint>
#include <vector>

#include "common/model/type.h"

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

struct TableRouteInfo {
    TableID table_id{-1};
    int32_t shard_count{0};
    std::vector<RouteInfo> routes;
};
}  // namespace adviskv::sdk