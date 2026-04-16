#include "sdm/background/replica_schedule_task.h"

#include <algorithm>
#include <unordered_set>

#include "common/define.h"
#include "common/log.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

void ReplicaScheduleTask::run() {
    Status status{Status::OK()};

    std::vector<TablePtr> table_list;
    status = sdm_store_->list_tables(table_list);
    if (status.fail()) {
        WARN("11");
        return;
    }

    for (TablePtr& table_ptr : table_list) {
        for (int i = 0; i < table_ptr->spec.shard_count; i++) {
            status = check_shard(*table_ptr, i);
            if (status.fail()) {
                WARN("22");
            }
        }
    }
}

Status ReplicaScheduleTask::check_shard(const Table& table, ShardID shard_id) {
    Status status{Status::OK()};

    std::vector<ReplicaPtr> replicas;
    status =
        sdm_store_->list_replicas_by_shard(table.table_id, shard_id, replicas);

    RETURN_IF_INVALID_STATUS(status)

    std::vector<ReplicaPtr> pending_replicas, ready_replicas;

    std::for_each(
        replicas.begin(), replicas.end(),
        [&pending_replicas, &ready_replicas](const ReplicaPtr& replica) {
            if (replica->state.status == ReplicaStatus::READY) {
                ready_replicas.push_back(replica);
            }
            if (replica->state.status == ReplicaStatus::PENDING and
                replica->state.assign_node_id.empty()) {
                pending_replicas.push_back(replica);
            }
        });

    if (pending_replicas.empty()) {
        return Status::OK();
    }
    
    std::unordered_set<NodeID> have_used_node_id_list;

    std::for_each(
        ready_replicas.begin(), ready_replicas.end(),
        [&have_used_node_id_list](const ReplicaPtr& replica) {
            have_used_node_id_list.insert(replica->state.assign_node_id);
        });

    ResourcePoolPtr resource_pool_ptr;
    status = sdm_store_->get_resource_pool(table.spec.resource_pool,
                                           resource_pool_ptr);
    RETURN_IF_INVALID_STATUS(status)
    RETURN_IF_INVALID_CONDITION(resource_pool_ptr != nullptr,
                            "resource pool should not be nullptr")

    std::vector<NodeID> node_id_list =
        resource_pool_ptr->nodes;  // 资源池里面的拥有的nodes

    std::vector<NodePtr> candi_nodes;
    for (NodeID node_id : node_id_list) {
        if (have_used_node_id_list.count(node_id)) {
            // 说明这些那些READY的replica占据了这个node
            continue;
        }
        NodePtr one;
        status = sdm_store_->get_node(node_id, one);
        RETURN_IF_INVALID_STATUS(status)
        if (one->state.status == NodeStatus::ONLINE) {
            candi_nodes.emplace_back(std::move(one));
        }
    }

    std::vector<NodePtr> selected_nodes;
    status = node_selector_.select_nodes(candi_nodes, pending_replicas.size(),
                                         selected_nodes);
    RETURN_IF_INVALID_STATUS(status)

    // 接下来把replica分配到node上
    RETURN_IF_INVALID_CONDITION(
        pending_replicas.size() == selected_nodes.size(),
        "pending replicas size should == selected_node size")

    int count = pending_replicas.size();
    for (int i = 0; i < count; i++) {
        NodePtr& node = selected_nodes[i];
        ReplicaPtr& replica = pending_replicas[i];
        // TODO 这里应该是把node的dc赋值过去吧？ 还有status设成ADDING
        replica->spec.dc = node->spec.dc;
        replica->state.assign_node_id = node->id;
        replica->state.endpoint = node->state.endpoint;
        replica->state.status = ReplicaStatus::ADDING;
        node->replicas.push_back(replica->replica_key);
    }

    return status;
}
}  // namespace adviskv::sdm