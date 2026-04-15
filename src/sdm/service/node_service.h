#pragma once
#include "common/status.h"
#include "sdm/manager/node_manager.h"
#include "sdm/model/service_param.h"

#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {


class NodeService{

public:

    explicit NodeService(SdmStore * sdm_store);

    Status register_node(const RegisterNodeParam& param);

private:
    // NodeManager* node_manager_;
    SdmStore * sdm_store_;


};

}