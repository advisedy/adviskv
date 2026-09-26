#pragma once

#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/reconcile/table_reconciler.h"

namespace adviskv::meta {

class MetaStore;

// Table / Database 的唯一写入入口。建删改走这里，phase 收敛也从这里进 TableReconciler。
class DdlService {
public:
    explicit DdlService(MetaStore* store);

    Status create_db(const CreateDBParam& param, Database* db);
    Status drop_db(const DropDBParam& param, Database* db);
    Status create_table(const CreateTableParam& param, Table* table);
    Status drop_table(const DropTableByNameParam& param, Table* table);
    Status alter_table_replica_count(const AlterTableReplicaCountParam& param, Table* table);
    Status get_table(const GetTableParam& param, Table* table);
    Status reconcile_all();

private:
    MetaStore* store_{nullptr};
    TableReconciler table_reconciler_;
};

}  // namespace adviskv::meta
