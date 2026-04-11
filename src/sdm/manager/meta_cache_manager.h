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
    DatabaseID db_id;
    int32_t shard_count;
    int32_t replica_count;
};
    

struct DBMetaCache{
    std::string db_name;
    DatabaseID db_id;
    std::string zone;
};

class MetaCacheManager{

    public:
        Status update_table_meta(const TableMetaCache& meta);
        Status get_table_meta(const std::string& db_name, const std::string& table_name, TableMetaCache* out) const;
        Status update_db_meta(const DBMetaCache& meta);
        Status get_db_meta(const std::string& db_name, DBMetaCache* out) const;
    private:
        std::shared_mutex caches_mutex_;
        std::unordered_map<TableMetaCacheKey, TableMetaCache, TableMetaCacheKeyHash> table_meta_caches_;
    };
        


}