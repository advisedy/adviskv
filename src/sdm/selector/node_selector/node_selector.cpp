#include "sdm/selector/node_selector/node_selector.h"

#include <fmt/format.h>

#include <algorithm>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/define.h"
#include "common/func.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {
namespace {

struct NodeView {
    Node node;
    int32 owned_replica_count{0};
    std::string dc;
};

bool is_valid_node(const Node& node) {
    if (node.state.endpoint.ip.empty() || node.state.endpoint.port <= 0) {
        return false;
    }
    return node.spec.status == NodeStatus::ONLINE;
}

bool better_candidate(
    const NodeView& lhs, const NodeView& rhs,
    const std::unordered_map<std::string, int>& selected_dc_counts) {
    //
    auto get_count = [&selected_dc_counts](const std::string& dc) -> int {
        auto it = selected_dc_counts.find(dc);
        if (it == selected_dc_counts.end()) return 0;
        return it->second;
    };

    int lhs_dc_count = get_count(lhs.dc);
    int rhs_dc_count = get_count(rhs.dc);

    if (lhs_dc_count != rhs_dc_count) {
        return lhs_dc_count < rhs_dc_count;
    }
    if (lhs.owned_replica_count != rhs.owned_replica_count) {
        return lhs.owned_replica_count < rhs.owned_replica_count;
    }
    return lhs.node.id < rhs.node.id;
}

std::vector<NodeView>::iterator select_one_node(
    std::vector<NodeView>& views,
    const std::unordered_set<NodeID>& selected_node_ids,
    const std::unordered_map<std::string, int32>& selected_dc_counts) {
    using NodeViewIt = std::vector<NodeView>::iterator;
    auto lower_priority = [&selected_dc_counts](NodeViewIt lhs,
                                                NodeViewIt rhs) {
        return better_candidate(*rhs, *lhs, selected_dc_counts);
    };
    std::priority_queue<NodeViewIt, std::vector<NodeViewIt>,
                        decltype(lower_priority)>
        candidates(lower_priority);

    for (auto it = views.begin(); it != views.end(); ++it) {
        if (selected_node_ids.count(it->node.id)) continue;
        candidates.push(it);
    }
    return candidates.empty() ? views.end() : candidates.top();
}

}  // namespace

Status DefaultNodeSelector::select_table_nodes(
    const PlaceNodesParam& param, TablePlacementResult& res) const {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store should not nullptr")

    std::vector<NodeView> views;
    std::vector<Node> candidate_nodes;
    Status status = store_->read_with([&](const SdmStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(txn.list_nodes_by_resource_pool(
            param.resource_pool, candidate_nodes))

        func::ad_erase_if(candidate_nodes, [](const Node& node) {
            return !is_valid_node(node);
        });

        if ((int32)(candidate_nodes.size()) < param.replica_count) {
            return Status::RESOURCE_EXHAUSTED(fmt::format(
                "not enough nodes in resource_pool '{}', need {} but have {}",
                param.resource_pool, param.replica_count,
                candidate_nodes.size()));
        }

        views.reserve(candidate_nodes.size());
        for (const Node& node : candidate_nodes) {
            std::vector<Replica> replicas;
            RETURN_IF_INVALID_STATUS(
                txn.list_replicas_by_node(node.id, replicas))

            NodeView view;
            view.node = node;
            view.owned_replica_count = std::count_if(
                replicas.begin(), replicas.end(), [](const Replica& replica) {
                    return replica.state.desired == ReplicaDesired::PRESENT;
                });
            view.dc = node.spec.dc;
            views.push_back(std::move(view));
        }
        return Status::OK();
    });
    RETURN_IF_INVALID_STATUS(status)

    res.shards.clear();
    res.shards.reserve(param.shard_count);
    for (int32_t shard_idx = 0; shard_idx < param.shard_count; ++shard_idx) {
        ShardPlacement shard;
        shard.shard_index = shard_idx;
        shard.nodes.reserve(param.replica_count);

        std::unordered_set<NodeID> selected_node_ids;
        std::unordered_map<std::string, int> selected_dc_counts;
        for (int32_t i = 0; i < param.replica_count; ++i) {
            auto it =
                select_one_node(views, selected_node_ids, selected_dc_counts);

            if (it == views.end()) {
                return Status::ERROR("select one node: it = views.end()");
            }

            shard.nodes.emplace_back(it->node);
            selected_node_ids.insert(it->node.id);
            ++selected_dc_counts[it->dc];
            it->owned_replica_count++;
        }
        res.shards.emplace_back(std::move(shard));
    }

    return Status::OK();
}

}  // namespace adviskv::sdm