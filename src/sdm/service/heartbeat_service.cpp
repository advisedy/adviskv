#include "sdm/service/heartbeat_service.h"

#include "common/define.h"
#include "sdm/model/service_param.h"

namespace adviskv::sdm {

HeartBeatService::HeartBeatService(SdmStore* sdm_store)
    : sdm_store_(sdm_store) {}

Status HeartBeatService::heartbeat(const HeartBeatParam& param,
                                   HeartBeatResult& result) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_INVALID_CONDITION(sdm_store_ != nullptr, "sdm_store is nullptr")

    Status status = update_node_state(param);
    RETURN_IF_INVALID_STATUS(status)

    status = apply_reported_replicas(param);
    RETURN_IF_INVALID_STATUS(status)

    status = build_desired_replicas(param.node_id, &result);
    RETURN_IF_INVALID_STATUS(status)

    return Status::OK();
}

Status HeartBeatService::update_node_state(const HeartBeatParam& param) {
    NodePtr node;
    Status status = sdm_store_->get_node(param.node_id, node);
    RETURN_IF_INVALID_STATUS(status)
    RETURN_IF_INVALID_CONDITION(node != nullptr, "node not found")

    //这里对于node的定义可能要变一下了，state里面的内容不全是代表着storage传过来的就要更新的。
    // 例如拥有的leader，这个应该是交给sdm的routeupdatechecker做的才对。 //TODO
    node->state.endpoint = Endpoint{param.ip, param.port};
    node->state.status = param.status;
    return Status::OK();
}

Status HeartBeatService::apply_reported_replicas(const HeartBeatParam& param) {
    for (const auto& info : param.replica_list) {
        ReplicaKey key{info.table_id, info.shard_id, info.replica_index};
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
    }
    return Status::OK();
}

Status HeartBeatService::build_desired_replicas(const NodeID& node_id,
                                                HeartBeatResult* result) {
    if (result == nullptr) {
        return Status::OK();
    }

    std::vector<ReplicaPtr> replicas;
    Status status = sdm_store_->list_replicas_by_node(node_id, replicas);
    RETURN_IF_INVALID_STATUS(status)

    result->entry_list.clear();
    for (const auto& replica : replicas) {
        if (!replica) {
            continue;
        }
        if (replica->spec.assign_node_id != node_id) {
            continue;
        }
        if (replica->state.status == ReplicaStatus::LOST) {
            continue;
        }

        HeartBeatResultEntry one;
        one.replica_key.table_id = replica->replica_key.table_id;
        one.replica_key.shard_id = replica->replica_key.shard_id;
        one.replica_key.replica_index = replica->replica_key.replica_index;
        one.replica_role = replica->spec.role;
        result->entry_list.push_back(std::move(one));
    }

    return Status::OK();
}

}  // namespace adviskv::sdm