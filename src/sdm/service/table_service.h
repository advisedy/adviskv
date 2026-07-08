#pragma once

#include "common/model/type.h"
#include "common/status.h"
#include "sdm/model/model.h"
#include "sdm/model/param.h"

namespace adviskv::sdm {

class SdmStore;
class SdmStoreTxn;

class TableService {
public:
    explicit TableService(SdmStore* store);

    Status place_table(const PlaceTableParam& param);
    Status drop_table(const DropTableParam& param);
    Status alter_table_replica_count(const AlterReplicaCountParam& param);
    Status get_table_meta(const GetTableMetaParam& param, Table* out_table) const;
    Status get_table_status(const GetTableStatusParam& param, Table* out_table) const;

    Status reconcile_all();

private:
    Status reconcile_table(Table& table);

    Status finalize_creating_table(Table& table);
    Status finalize_resizing_table(Table& table);
    Status finalize_table_until_ready(Table& table, TablePhase waiting_phase);
    Status ensure_all_shards_ok(SdmStoreTxn& txn, Table& table, bool& all_shards_ok);

    Status finalize_absent_table(Table& table);
    Status ensure_all_shards_deleted(SdmStoreTxn& txn, Table& table, bool& all_shards_deleted);

    SdmStore* store_{nullptr};
};

}  // namespace adviskv::sdm