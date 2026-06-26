#include "sdm/service/node_service.h"

#include <algorithm>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/sdm_store_txn.h"

namespace adviskv::sdm {

namespace {
constexpr size_t kHeartbeatCheckBatchSize = 16;
}

NodeService::NodeService(SdmStore* store)
    : store_(store), start_ts_ms_(func::get_current_ts_ms()) {}

Status NodeService::register_node(const RegisterNodeParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    Node node;
    node.id = param.node_id;
    node.meta.dc = param.dc;
    node.meta.resource_pool = param.resource_pool;
    node.state.status = NodeStatus::ONLINE;
    node.state.endpoint = Endpoint{param.ip, param.port};
    node.state.last_heartbeat_ts = param.last_heartbeat_ts;
    return store_->write_with(
        [&](SdmStoreTxn& txn) { return txn.put_node(node); });
}

Status NodeService::heartbeat(const HeartBeatParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    Status status = store_->write_with([&](SdmStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(update_node_heartbeat(txn, param))
        return apply_reported_replicas(txn, param);
    });
    RETURN_IF_INVALID_STATUS(status)

    LOG_DEBUG(
        "[NodeService] get node_id:{} heartbeat, port:{}, "
        "replica_list_size:{}",
        param.node_id, param.port, param.replica_list.size());
    return Status::OK();
}

Status NodeService::update_node_heartbeat(SdmStoreTxn& txn,
                                          const HeartBeatParam& param) {
    NodeOr node;
    RETURN_IF_INVALID_STATUS(txn.get_node(param.node_id, node))
    RETURN_IF_INVALID_CONDITION(!node.is_empty(), "node not found")

    node->state.endpoint = Endpoint{param.ip, param.port};
    node->state.last_heartbeat_ts = param.last_heartbeat_ts;
    return txn.put_node(*node);
}

Status NodeService::apply_reported_replicas(SdmStoreTxn& txn,
                                            const HeartBeatParam& param) {
    for (const auto& info : param.replica_list) {
        ReplicaID key = info.replica_id;
        ReplicaOr replica;
        RETURN_IF_INVALID_STATUS(txn.get_replica(key, replica))

        if (replica.is_empty()) {
            continue;
        }
        if (replica->spec.assign_node_id != param.node_id) {
            LOG_WARN("[NodeService] replica:assign_node_id != node_id");
            continue;
        }

        replica->state.observed_raft_role = info.role;
        replica->state.observed_endpoint = Endpoint{param.ip, param.port};
        replica->state.observed_storage_status = info.storage_status;
        replica->state.term = info.term;
        replica->state.update_ts = func::get_current_ts_ms();
        RETURN_IF_INVALID_STATUS(txn.put_replica(*replica))

        if (replica->state.observed_raft_role == ReplicaRole::LEADER) {
            LOG_DEBUG("[NodeService] replica_id:{} is leader",
                      replica->replica_id.to_string());
        }
    }
    return Status::OK();
}

Status NodeService::reconcile_all() {
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    std::vector<Node> nodes;
    Status status = store_->read_with(
        [&](const SdmStoreTxn& txn) { return txn.list_nodes(nodes); });
    RETURN_IF_INVALID_STATUS(status)

    std::vector<NodeID> node_ids;
    node_ids.reserve(nodes.size());
    for (const auto& node : nodes) {
        node_ids.push_back(node.id);
    }

    // 这里给node搞批处理，防止持有锁的时间太久了
    for (size_t begin = 0; begin < node_ids.size();
         begin += kHeartbeatCheckBatchSize) {
        size_t end =
            std::min(begin + kHeartbeatCheckBatchSize, node_ids.size());
        status = store_->write_with([&](SdmStoreTxn& txn) -> Status {
            for (size_t index = begin; index < end; ++index) {
                NodeOr current;
                RETURN_IF_INVALID_STATUS(txn.get_node(node_ids[index], current))
                if (current.is_empty()) {
                    continue;
                }
                Node node = *current;
                Status s = check_and_modify_node(txn, node);
                if (s.fail()) {
                    LOG_WARN(
                        "[NodeService] check node failed, node_id:{}, msg:{}",
                        node_ids[index], s.msg());
                }
            }
            return Status::OK();
        });
        if (status.fail()) {
            LOG_WARN("[NodeService] node check batch failed, begin:{}, msg:{}",
                     begin, status.msg());
        }
    }
    return Status::OK();
}

Status NodeService::check_and_modify_node(SdmStoreTxn& txn, Node& node) {
    int64_t current_ts_ms = func::get_current_ts_ms();
    int64_t diff = current_ts_ms - node.state.last_heartbeat_ts;
    bool in_startup_grace =
        (current_ts_ms - start_ts_ms_ <= HEARTBEAT_STARTUP_GRACE_MS);

    if (diff <= HEARTBEAT_SUSPECT_TIMEOUT_MS) {
        if (node.state.status != NodeStatus::ONLINE) {
            return mark_node_online(txn, node);
        }
        return Status::OK();
    }

    if (in_startup_grace) {
        return Status::OK();
    }

    if (diff > HEARTBEAT_OFFLINE_TIMEOUT_MS) {
        if (node.state.status != NodeStatus::OFFLINE) {
            return mark_node_offline(txn, node);
        }
    } else {
        if (node.state.status == NodeStatus::ONLINE) {
            return mark_node_suspect(txn, node);
        }
    }

    return Status::OK();
}

Status NodeService::mark_node_offline(SdmStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeService] node offline, node_id:{}", node.id);
    node.state.status = NodeStatus::OFFLINE;
    return txn.put_node(node);
}

Status NodeService::mark_node_suspect(SdmStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeService] node suspect, node_id:{}", node.id);
    node.state.status = NodeStatus::SUSPECT;
    return txn.put_node(node);
}

Status NodeService::mark_node_online(SdmStoreTxn& txn, Node& node) {
    LOG_INFO("[NodeService] node online, node_id:{}", node.id);
    node.state.status = NodeStatus::ONLINE;
    return txn.put_node(node);
}

Status NodeService::check_and_modify_node_for_test(Node& node) {
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    return store_->write_with([&](SdmStoreTxn& txn) -> Status {
        NodeOr current;
        RETURN_IF_INVALID_STATUS(txn.get_node(node.id, current))
        if (current.is_empty()) {
            return Status::OK();
        }
        node = *current;
        return check_and_modify_node(txn, node);
    });
}

}  // namespace adviskv::sdm