#pragma once

#include <cstdint>
#include <string>
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

struct RouteCacheKey {
    std::string db_name;
    std::string table_name;
    Key key;

    bool operator==(const RouteCacheKey& other) const {
        return db_name == other.db_name && table_name == other.table_name &&
               key == other.key;
    }
};

struct RouteCacheKeyHash {
    size_t operator()(const RouteCacheKey& cache_key) const {
        size_t h1 = std::hash<std::string>{}(cache_key.db_name);
        size_t h2 = std::hash<std::string>{}(cache_key.table_name);
        size_t h3 = std::hash<Key>{}(cache_key.key);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

struct CachedRoute {
    RouteInfo route;
    int64_t expire_ts_ms{0};
};

}  // namespace adviskv::sdk