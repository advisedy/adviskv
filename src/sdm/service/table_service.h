#pragma once

#include "common/status.h"
#include "sdm/client/storage_client.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include "sdm/selector/node_selector/node_selector.h"

namespace adviskv::sdm {

class TableService {
   public:
    TableService(SdmStore* sdm_store, StorageClient* storage_client, NodeSelector* node_selector);

    Status place_table(const PlaceTableParam& param);

   private:
    SdmStore* sdm_store_;
    StorageClient* storage_client_;
    NodeSelector* node_selector_;
};

}  // namespace adviskv::sdm