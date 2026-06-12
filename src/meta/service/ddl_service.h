#pragma once

#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/service/ddl_params.h"
#include "meta/service/sdm_client.h"

namespace adviskv::meta {

class DdlService {
   public:
    explicit DdlService(CatalogManager* catalog_manager,
                        ISdmClient* sdm_client);

    // 负责一些会涉及到catalog和sdm的操作
    Status create_table(const CreateTableParam& param, TableMeta* table_meta);
    Status drop_table(const DropTableParam& param, TableMeta* table_meta);
    Status create_db(const CreateDBParam& param, DBMeta* db_meta);
    Status drop_db(const DropDBParam& param, DBMeta* db_meta);
    Status get_table(const GetTableParam& param, TableMeta* table_meta);

   private:
    ISdmClient* sdm_client_;
    CatalogManager* catalog_manager_;
};

}  // namespace adviskv::meta