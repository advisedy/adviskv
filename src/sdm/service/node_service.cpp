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
    node.spec.dc = param.dc;
    node.spec.resource_pool = param.resource_pool;
    node.spec.status = NodeStatus::ONLINE;
    node.state.endpoint = Endpoint{param.ip, param.port};
    node.state.last_heartbeat_ts = param.last_heartbeat_ts;
    return store_->write_with(
        [&](SdmStoreTxn& txn) { return txn.put_node(node); });
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
    bool in_startup_grace = (current_ts_ms - start_ts_ms_ <= STARTUP_GRACE_MS);

    if (diff <= SUSPECT_TIMEOUT_MS) {
        if (node.spec.status != NodeStatus::ONLINE) {
            return mark_node_online(txn, node);
        }
        return Status::OK();
    }

    if (in_startup_grace) {
        return Status::OK();
    }

    if (diff > OFFLINE_TIMEOUT_MS) {
        if (node.spec.status != NodeStatus::OFFLINE) {
            return mark_node_offline(txn, node);
        }
    } else {
        if (node.spec.status == NodeStatus::ONLINE) {
            return mark_node_suspect(txn, node);
        }
    }

    return Status::OK();
}

Status NodeService::mark_node_offline(SdmStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeService] node offline, node_id:{}", node.id);
    node.spec.status = NodeStatus::OFFLINE;
    return txn.put_node(node);
}

Status NodeService::mark_node_suspect(SdmStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeService] node suspect, node_id:{}", node.id);
    node.spec.status = NodeStatus::SUSPECT;
    return txn.put_node(node);
}

Status NodeService::mark_node_online(SdmStoreTxn& txn, Node& node) {
    LOG_INFO("[NodeService] node online, node_id:{}", node.id);
    node.spec.status = NodeStatus::ONLINE;
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