#pragma once

#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include "sdm/workflow/placetable_workflow.h"

namespace adviskv::sdm {

class TableService {
   public:
    TableService(SdmStore* store, PlaceTableWorkflow* workflow);

    Status place_table(const PlaceTableParam& param);
    Status get_table_status(const GetTableStatusParam& param, Table* table);

   private:
    SdmStore* store_;
    PlaceTableWorkflow* workflow_;
};

}  // namespace adviskv::sdm