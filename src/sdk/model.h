#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/model/model.h"
#include "common/shard_range.h"

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
    HashRange hash_range{};
};

struct TableRouteInfo {
    TableID table_id{-1};
    int32_t shard_count{0};
    std::vector<RouteInfo> routes;
};

inline Status check_table_routes_is_valid(TableRouteInfo& table) {
    if (table.table_id < 0 || table.shard_count <= 0 || table.routes.size() != static_cast<size_t>(table.shard_count)) {
        return Status::INVALID_ARGUMENT("invalid table route count or table ID");
    }
    std::sort(table.routes.begin(), table.routes.end(),
              [](const RouteInfo& a, const RouteInfo& b) { return a.hash_range.start < b.hash_range.start; });
    std::vector<ShardRange> ranges;
    for (size_t i = 0; i < table.routes.size(); ++i) {
        RouteInfo& route = table.routes[i];
        if (route.table_id != table.table_id) {
            return Status::INVALID_ARGUMENT("invalid");
        }
        ranges.push_back({route.shard_id, route.hash_range});
    }
    return check_shard_ranges_is_valid(table.shard_count, ranges);
}

inline size_t locate_route(const TableRouteInfo& table, std::string_view key) {
    std::vector<ShardRange> ranges;
    for (const RouteInfo& route : table.routes) {
        ranges.push_back({route.shard_id, route.hash_range});
    }
    return find_shard_range(ranges, hash_bucket_for_key(key));
}
}  // namespace adviskv::sdk