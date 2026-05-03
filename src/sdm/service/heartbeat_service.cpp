#include "sdm/service/heartbeat_service.h"

#include "common/define.h"
#include "common/func.h"
#include "sdm/model/service_param.h"
namespace adviskv::sdm {

HeartBeatService::HeartBeatService(SdmStore* sdm_store)
    : sdm_store_(sdm_store) {}

Status HeartBeatService::heartbeat(const HeartBeatParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_INVALID_CONDITION(sdm_store_ != nullptr, "sdm_store is nullptr")

    Status status = update_node_state(param);
    RETURN_IF_INVALID_STATUS(status)

    status = apply_reported_replicas(param);
    RETURN_IF_INVALID_STATUS(status)

    // status = build_desired_replicas(param.node_id, &result);
    // RETURN_IF_INVALID_STATUS(status)

    return Status::OK();
}

Status HeartBeatService::update_node_state(const HeartBeatParam& param) {
    NodePtr node;
    Status status = sdm_store_->get_node(param.node_id, node);
    RETURN_IF_INVALID_STATUS(status)
    RETURN_IF_INVALID_CONDITION(node != nullptr, "node not found")

    // 这里对于node的定义可能要变一下了，state里面的内容不全是代表着storage传过来的就要更新的。
    //  例如拥有的leader，这个应该是交给sdm的routeupdatechecker做的才对。 //TODO
    node->state.endpoint = Endpoint{param.ip, param.port};
    node->state.last_heartbeat_ts = param.last_heartbeat_ts;
    return sdm_store_->put_node(*node);
}

Status HeartBeatService::apply_reported_replicas(const HeartBeatParam& param) {
    for (const auto& info : param.replica_list) {
        ReplicaID key{info.shard_id.table_id, info.shard_id.shard_index,
                      info.replica_index};
        ReplicaPtr replica;
        Status status = sdm_store_->get_replica(key, replica);
        RETURN_IF_INVALID_STATUS(status)

        if (!replica) {
            continue;
        }
        if (replica->spec.assign_node_id != param.node_id) {
            continue;
        }

        replica->state.role = info.role;
        replica->state.endpoint = Endpoint{param.ip, param.port};

        if (replica->spec.status == ReplicaStatus::ADDING &&
            info.status == ReplicaStatus::READY) {
            replica->spec.status = ReplicaStatus::READY;
        } else {
            replica->spec.status = info.status;
        }
        status = sdm_store_->put_replica(*replica);
        RETURN_IF_INVALID_STATUS(status)
    }
    return Status::OK();
}


}  // namespace adviskv::sdm
