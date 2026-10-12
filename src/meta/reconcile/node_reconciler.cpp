#include "meta/reconcile/node_reconciler.h"

#include <algorithm>

#include "common/constants.h"
#include "common/func.h"
#include "common/log.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

NodeReconciler::NodeReconciler(MetaStore* store) : NodeReconciler(store, func::get_current_ts_ms()) {}

NodeReconciler::NodeReconciler(MetaStore* store, int64_t start_ts_ms)
        : store_(store), start_ts_ms_(start_ts_ms) {}

Status NodeReconciler::run() {
    if (store_ == nullptr) {
        Status error{StatusCode::INVALID_ARGUMENT, "store is nullptr"};
        LOG_ERROR("[NodeReconciler] reconcile skipped, msg={}", error.msg());
        return error;
    }

    std::vector<Node> nodes;
    Status status = store_->read_with([&](const MetaStoreTxn& txn) { return txn.list_nodes(nodes); });
    if (status.fail()) {
        LOG_WARN("[NodeReconciler] list nodes failed, msg={}", status.msg());
        return status;
    }

    std::vector<NodeID> node_ids;
    node_ids.reserve(nodes.size());
    for (const auto& node : nodes) {
        node_ids.push_back(node.id);
    }

    // 分批处理，避免长时间持锁。
    for (size_t begin = 0; begin < node_ids.size(); begin += K_META_HEARTBEAT_CHECK_BATCH_SIZE) {
        size_t end = std::min(begin + K_META_HEARTBEAT_CHECK_BATCH_SIZE, node_ids.size());
        status = store_->write_with([&](MetaStoreTxn& txn) -> Status {
            for (size_t index = begin; index < end; ++index) {
                NodeOr current;
                RETURN_IF_INVALID_STATUS(txn.get_node(node_ids[index], current))
                if (current.is_empty()) {
                    continue;
                }
                Node node = *current;
                Status s = check_and_modify_node(txn, node);
                if (s.fail()) {
                    LOG_WARN("[NodeReconciler] check node failed, node_id:{}, msg:{}", node_ids[index], s.msg());
                }
            }
            return Status::OK();
        });
        if (status.fail()) {
            LOG_WARN("[NodeReconciler] node check batch failed, begin:{}, msg:{}", begin, status.msg());
        }
    }
    return Status::OK();
}

Status NodeReconciler::check_and_modify_node(MetaStoreTxn& txn, Node& node) {
    if (node.state.offline_reason == NodeOfflineReason::TOPOLOGY_MISMATCH) {
        if (node.state.status != NodeStatus::OFFLINE) {
            node.state.status = NodeStatus::OFFLINE;
            return txn.put_node(node);
        }
        return Status::OK();
    }

    int64_t current_ts_ms = func::get_current_ts_ms();
    int64_t diff = current_ts_ms - node.state.last_heartbeat_ts;
    bool in_startup_grace = (current_ts_ms - start_ts_ms_ <= K_META_HEARTBEAT_STARTUP_GRACE_MS);

    if (diff <= K_META_HEARTBEAT_SUSPECT_TIMEOUT_MS) {
        if (node.state.status != NodeStatus::ONLINE) {
            return mark_node_online(txn, node);
        }
        return Status::OK();
    }

    if (in_startup_grace) {
        return Status::OK();
    }

    if (diff > K_META_HEARTBEAT_OFFLINE_TIMEOUT_MS) {
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

Status NodeReconciler::mark_node_offline(MetaStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeReconciler] node offline, node_id:{}", node.id);
    node.state.status = NodeStatus::OFFLINE;
    return txn.put_node(node);
}

Status NodeReconciler::mark_node_suspect(MetaStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeReconciler] node suspect, node_id:{}", node.id);
    node.state.status = NodeStatus::SUSPECT;
    return txn.put_node(node);
}

Status NodeReconciler::mark_node_online(MetaStoreTxn& txn, Node& node) {
    LOG_INFO("[NodeReconciler] node online, node_id:{}", node.id);
    node.state.status = NodeStatus::ONLINE;
    return txn.put_node(node);
}

}  // namespace adviskv::meta
