#pragma once

#include "common/status.h"
#include "meta/model/meta_types.h"

namespace adviskv::meta {

struct PersistedMetaRecord {
    std::unordered_map<DatabaseID, DBMeta> db_meta_map;
    std::unordered_map<TableID, TableMeta> table_id2table_meta;
    DatabaseID next_db_id{0};
    TableID next_table_id{0};
};

class IMetaPersistEngine {
   public:
    virtual ~IMetaPersistEngine() = default;

    virtual Status init() = 0;
    virtual Status close() = 0;

    virtual Status save_meta(const PersistedMetaRecord& record) = 0;
    virtual Status load_meta(PersistedMetaRecord& record) = 0;
};

}  // namespace adviskv::meta
