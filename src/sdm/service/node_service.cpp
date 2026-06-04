#include "sdm/service/node_service.h"

#include "common/define.h"
#include "common/func.h"
#include "common/status.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

NodeService::NodeService(SdmStore* sdm_store) : sdm_store_(sdm_store) {}

Status NodeService::register_node(const RegisterNodeParam& param) {
    RETURN_IF_INVALID_PARAM(param)

    Node node;
    node.id = param.node_id;
    node.spec.dc = param.dc;
    node.spec.resource_pool = param.resource_pool;
    node.spec.status = NodeStatus::ONLINE;
    node.state.endpoint = Endpoint{param.ip, param.port};
    node.state.last_heartbeat_ts = param.last_heartbeat_ts;
    Status status = sdm_store_->put_node(node);

    return status;
}

}  // namespace adviskv::sdm