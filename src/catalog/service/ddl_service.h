#pragma once

#include "common/status.h"
#include "catalog/store/catalog_store.h"
#include "catalog/model/param.h"
#include "catalog/service/topo_client.h"

namespace adviskv::catalog {

class DdlService {
public:
    explicit DdlService(CatalogStore* catalog_store, ITopoClient* topo_client);

    // 负责一些会涉及到catalog和topo的操作
    Status create_table(const CreateTableParam& param, TableDesc* table_desc);
    Status drop_table(const DropTableParam& param, TableDesc* table_desc);
    Status alter_table_replica_count(const AlterTableReplicaCountParam& param, TableDesc* table_desc);
    Status create_db(const CreateDBParam& param, DatabaseDesc* db_desc);
    Status drop_db(const DropDBParam& param, DatabaseDesc* db_desc);
    Status get_table(const GetTableParam& param, TableDesc* table_desc);

private:
    ITopoClient* topo_client_;
    CatalogStore* catalog_store_;
};

}  // namespace adviskv::catalog
