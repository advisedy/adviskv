#pragma once

#include "common/status.h"
#include "catalog/model/model.h"

namespace adviskv::catalog {

struct CatalogPersistedRecord {
    std::unordered_map<DatabaseID, DatabaseDesc> db_desc_map;
    std::unordered_map<TableID, TableDesc> table_id2table_desc;
    DatabaseID next_db_id{0};
    TableID next_table_id{0};
};

class ICatalogPersistEngine {
public:
    virtual ~ICatalogPersistEngine() = default;

    virtual Status init() = 0;
    virtual Status close() = 0;

    virtual Status save_catalog(const CatalogPersistedRecord& record) = 0;
    virtual Status load_catalog(CatalogPersistedRecord& record) = 0;
};

}  // namespace adviskv::catalog
