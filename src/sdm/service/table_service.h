#pragma once

#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include "sdm/workflow/placetable_workflow.h"

namespace adviskv::sdm {

class TableService {
   public:
    explicit TableService(PlaceTableWorkflow* workflow);

    Status place_table(const PlaceTableParam& param);

   private:
    PlaceTableWorkflow* workflow_;
};

}  // namespace adviskv::sdm