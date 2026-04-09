#pragma once

#include "common/status.h"
#include "common/type.h"
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
namespace adviskv{

struct TableMetaCacheKey{
    std::string db_name;
    std::string table_name;
};

struct TableMetaCacheKeyHash{
    uint64_t operator()(const TableMetaCacheKey& key) const{
        std::string res = key.db_name + "***&&**" + key.table_name;
        return std::hash<std::string>{}(res);
    }
};

struct TableMetaCache {
    std::string db_name;
    std::string table_name;
    TableID table_id;
    int32_t shard_count;
    int32_t replica_count;
};


enum class ReplicaRole{
    LEADER = 1,
    FOLLOWER = 2,
    PRE_LEADER = 3,
    PRE_FOLLOWER = 4
};

struct ReplicaLocation{
    int32_t replica_index;
    NodeID node_id;
    std::string ip;
    int32_t port;
    ReplicaRole role;
};


struct ShardRouteKey{
    TableID table_id;
    ShardID shard_id;
};

struct ShardRouteKeyHash{
    uint64_t operator()(const ShardRouteKey& id) const {
        return std::hash<uint64_t>{}(id.table_id)* 1000 + std::hash<uint64_t>{}(id.shard_id);
    };
};

struct ShardRoute{
    TableID table_id;
    ShardID shard_id;    
    std::vector<ReplicaLocation> replicas;
};

class RouteManager{

public:
    Status update_table_meta(const TableMetaCache& meta);
    Status get_table_meta(const std::string& db_name, const std::string& table_name, TableMetaCache* out) const;
    Status update_route(const ShardRoute& route);
    Status get_route(TableID table_id, ShardID shard_id, ShardRoute* out) const;

private:
    std::shared_mutex routes_mutex_;
    std::unordered_map<ShardRouteKey, ShardRoute, ShardRouteKeyHash> routes_;
    
    std::shared_mutex caches_mutex_;
    std::unordered_map<TableMetaCacheKey, TableMetaCache, TableMetaCacheKeyHash> table_meta_caches_;
};

}
