#pragma once
#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"

namespace adviskv::sdm {

class NodeService {
   public:
    explicit NodeService(SdmStore* sdm_store);

    Status register_node(const RegisterNodeParam& param);

   private:
    SdmStore* sdm_store_;
};

}  // namespace adviskv::sdm