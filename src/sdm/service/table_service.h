#pragma once

#include "common/status.h"

#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm{


class TableService{

public:
    TableService();
  
    Status place_table(const PlaceTableParam& param);

private:
    SdmStore * sdm_store;
};

}