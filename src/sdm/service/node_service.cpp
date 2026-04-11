#include "sdm/service/node_service.h"
#include "common/define.h"
#include "common/status.h"
#include "sdm/manager/node_manager.h"


namespace adviskv {

Status NodeService::register_node(const RegisterNodeParam& param){

    NodeMeta node_meta;
    Status status = node_manager_->register_node(param, &node_meta);

    RETURN_IF_INVALID_STATUS(status)

    
    return Status::OK();
}

}