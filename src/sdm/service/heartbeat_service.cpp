#include "sdm/service/heartbeat_service.h"

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "sdm/model/service_param.h"
#include "sdm/utility/enum_convert.h"
namespace adviskv::sdm {

HeartBeatService::HeartBeatService(SdmStore* sdm_store)
    : sdm_store_(sdm_store) {}

Status HeartBeatService::heartbeat(const HeartBeatParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(sdm_store_, "sdm_store is nullptr")

    Status status = update_node_state(param);
    RETURN_IF_INVALID_STATUS(status)

    status = apply_reported_replicas(param);
    RETURN_IF_INVALID_STATUS(status)

    // status = build_desired_replicas(param.node_id, &result);
    // RETURN_IF_INVALID_STATUS(status)

    return Status::OK();
}

Status HeartBeatService::update_node_state(const HeartBeatParam& param) {
    NodeOr node;
    Status status = sdm_store_->get_node(param.node_id, node);
    RETURN_IF_INVALID_STATUS(status)
    RETURN_IF_INVALID_CONDITION(!node.is_empty(), "node not found")

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
        ReplicaOr replica;
        Status status = sdm_store_->get_replica(key, replica);
        RETURN_IF_INVALID_STATUS(status)

        if (replica.is_empty()) {
            continue;
        }
        if (replica->spec.assign_node_id != param.node_id) {
            LOG_WARN("HeartBeatService: replica:aggsin_node_id != node_id");
            continue;
        }

        replica->state.observed_role = info.role;
        replica->state.observed_endpoint = Endpoint{param.ip, param.port};
        ReplicaPhase phase;
        RETURN_IF_INVALID_CONDITION(
            convert_replica_status_to_phase(info.status, phase),
            "replica status is not valid")
        replica->state.phase = phase;
        replica->state.update_ts = func::get_current_ts_ms();
        replica->state.term = info.term;
        status = sdm_store_->put_replica(*replica);
        RETURN_IF_INVALID_STATUS(status)
    }
    return Status::OK();
}

}  // namespace adviskv::sdm