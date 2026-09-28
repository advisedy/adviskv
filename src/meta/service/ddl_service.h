#pragma once

#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/reconcile/table_reconciler.h"

namespace adviskv::meta {

class MetaStore;

// Table / Database 的建删改入口。
class DdlService {
public:
    explicit DdlService(MetaStore* store);

    Status reconcile_all();
    Status create_db(const CreateDBParam& param, Database* db);
    Status drop_db(const DropDBParam& param, Database* db);
    Status create_table(const CreateTableParam& param, Table* table);
    Status drop_table(const DropTableByNameParam& param, Table* table);
    Status alter_table_replica_count(const AlterTableReplicaCountParam& param, Table* table);
    Status get_table(const GetTableParam& param, Table* table);

private:
    MetaStore* store_{nullptr};
    TableReconciler reconciler_;
};

}  // namespace adviskv::meta
