#pragma once

#include <cstdint>
#include <vector>

#include "common/type.h"

namespace adviskv::sdk {

enum class RouteReplicaRole : int8 {
    LEADER = 0,
    FOLLOWER = 1,
    UNKNOWN = 2,
};

struct RouteReplica {
    Endpoint endpoint;
    RouteReplicaRole role{RouteReplicaRole::UNKNOWN};
};

struct RouteInfo {
    TableID table_id{-1};
    ShardIndex shard_id{-1};
    std::vector<RouteReplica> replicas;
};

}  // namespace adviskv::sdk