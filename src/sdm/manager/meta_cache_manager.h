#pragma once

#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "common/type.h"


namespace adviskv {

class MetaCacheManager {
    // public:
    //     Status update_table_meta(const TableMetaCache& meta);
    //     Status get_table_meta(const std::string& db_name, const std::string&
    //     table_name, TableMetaCache* out) const; Status update_db_meta(const
    //     DBMetaCache& meta); Status get_db_meta(const std::string& db_name,
    //     DBMetaCache* out) const;

    // private:
    //     mutable std::shared_mutex table_caches_mutex_;
    //     mutable std::shared_mutex db_caches_mutex_;
    //     std::unordered_map<TableMetaCacheKey, TableMetaCache,
    //     TableMetaCacheKeyHash> table_meta_caches_;
    //     std::unordered_map<std::string, DBMetaCache> db_meta_caches_;
};

}  // namespace adviskv