#pragma once

#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/type.h"
#include "meta/catalog/meta_types.h"

namespace adviskv::meta {

struct PersistedMetaRecord {
    std::unordered_map<DatabaseID, DBMeta> db_meta_map;
    std::unordered_map<TableID, TableMeta> table_id2table_meta;
    DatabaseID next_db_id{0};
    TableID next_table_id{0};
};

class MetaPersistEngine {
   public:
    explicit MetaPersistEngine(const std::string& data_dir);
    ~MetaPersistEngine();

    Status init();
    Status close();

    Status save_meta(const PersistedMetaRecord& record);
    Status load_meta(PersistedMetaRecord& record);

   private:
    std::string data_dir_;
    std::string meta_data_path_;
    std::string meta_data_tmp_path_;
};

}  // namespace adviskv::meta
