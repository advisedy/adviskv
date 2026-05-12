#include "sdm/service/node_service.h"

#include "common/define.h"
#include "common/func.h"
#include "common/status.h"
#include "sdm/manager/node_manager.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

NodeService::NodeService(SdmStore* sdm_store) : sdm_store_(sdm_store) {}

Status NodeService::register_node(const RegisterNodeParam& param) {
    RETURN_IF_INVALID_PARAM(param)

    //   NodeMeta node_meta;
    //   Status status = node_manager_->register_node(
    //       param.node_id, param.ip, param.port, param.zone, &node_meta);

    //   RETURN_IF_INVALID_STATUS(status)

    //   return Status::OK();
    Node node{
        .id = param.node_id,
        .spec.dc = param.dc,
        .spec.resource_pool = param.resource_pool,
        .spec.status = NodeStatus::ONLINE,
        .state.endpoint = {param.ip, param.port},
        .state.last_heartbeat_ts = param.last_heartbeat_ts,
    };
    Status status = sdm_store_->put_node(node);

    return status;
}

}  // namespace adviskv::sdm
