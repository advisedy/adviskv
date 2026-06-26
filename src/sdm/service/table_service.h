#pragma once

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

class SdmStore;
class SdmStoreTxn;

class TableService {
   public:
    explicit TableService(SdmStore* store);

    Status place_table(const PlaceTableParam& param);
    Status drop_table(const DropTableParam& param);
    Status get_table_status(const GetTableStatusParam& param,
                            Table* out_table) const;

    Status reconcile_all();

   private:
    Status reconcile_table(Table& table);

    Status finalize_present_table(Table& table);
    Status ensure_all_shards_ok(SdmStoreTxn& txn, Table& table,
                                bool& all_shards_ok);

    Status finalize_absent_table(Table& table);
    Status ensure_all_shards_deleted(SdmStoreTxn& txn, Table& table,
                                     bool& all_shards_deleted);

    Status mark_table_error(Table& table, const Status& status);

    SdmStore* store_{nullptr};
};

}  // namespace adviskv::sdm