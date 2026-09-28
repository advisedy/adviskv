#pragma once

#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

class MetaStore;
class MetaStoreTxn;
class DdlService;

// 只更新 Table phase，不负责副本放置。
class TableReconciler {
private:
    friend class DdlService;

    explicit TableReconciler(MetaStore* store);

    Status run();
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
