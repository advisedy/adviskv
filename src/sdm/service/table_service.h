#pragma once

#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

class TableService {
   public:
    explicit TableService(SdmStore* store);

    Status place_table(const PlaceTableParam& param);
    Status drop_table(const DropTableParam& param);
    Status get_table_status(const GetTableStatusParam& param, Table* table);

   private:
    SdmStore* store_;
};

}  // namespace adviskv::sdm