#pragma once
#include "common/status.h"
#include "sdm/manager/node_manager.h"
#include "sdm/model/service_param.h"
namespace adviskv {


class NodeService{

public:

    explicit NodeService(NodeManager* node_manager);

    Status register_node(const RegisterNodeParam& param);

private:
    NodeManager* node_manager_;

};

}