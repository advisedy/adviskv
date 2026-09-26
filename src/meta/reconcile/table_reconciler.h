#pragma once

#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

class MetaStore;
class MetaStoreTxn;

// 把 Table 的 phase 往终态推：CREATING/RESIZING -> READY，DELETING -> DELETED。
// 只改 Table 自己，不负责放副本，那是 ReplicaGroupService 的事。
class TableReconciler {
public:
    explicit TableReconciler(MetaStore* store);

    Status reconcile_all();

private:
    Status reconcile_table(Table& table);
    Status finalize_creating_table(Table& table);
    Status finalize_resizing_table(Table& table);
    Status finalize_table_until_ready(Table& table, TablePhase waiting_phase);
    Status ensure_all_shards_ok(MetaStoreTxn& txn, Table& table, bool& all_shards_ok);
    Status finalize_absent_table(Table& table);
    Status ensure_all_shards_deleted(MetaStoreTxn& txn, Table& table, bool& all_shards_deleted);

    MetaStore* store_{nullptr};
};

}  // namespace adviskv::meta
