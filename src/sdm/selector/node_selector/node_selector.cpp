#include "sdm/selector/node_selector/node_selector.h"

#include <fmt/format.h>

#include <algorithm>
#include <string>

#include "common/define.h"
#include "common/func.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/node_manager.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

Status DefaultNodeSelector::select_table_nodes(
    const PlaceNodesParam& param, TablePlacementResult& res) const {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_INVALID_CONDITION(store_ != nullptr, "store should not nullptr")

    std::vector<NodePtr> candidate_nodes;
    Status status = store_->list_nodes_by_resource_pool(param.resource_pool,
                                                        candidate_nodes);
    RETURN_IF_INVALID_STATUS(status)

    // std::vector<NodePtr> candidate_nodes;
    // candidate_nodes.reserve(pool_nodes.size());
    // for (const NodePtr& node : pool_nodes) {
    //     if (!node) {
    //         continue;
    //     }
    //     if (node->spec.status != NodeStatus::ONLINE) {
    //         continue;
    //     }
    //     if (node->state.endpoint.ip.empty() || node->state.endpoint.port <=
    //     0) {
    //         continue;
    //     }
    //     candidate_nodes.emplace_back(node);
    // }

    func::ad_erase_if(candidate_nodes, [](const NodePtr& node) {
        if (!node) return true;
        if (node->state.endpoint.ip.empty() or node->state.endpoint.port <= 0)
            return true;
        if (node->spec.status != NodeStatus::ONLINE) return true;
        return false;
    });

    RETURN_IF_INVALID_CONDITION(
        (int32)(candidate_nodes.size()) >= param.replica_count,
        fmt::format(
            "not enough nodes in resource_pool '{}', need {} but have {}",
            param.resource_pool, param.replica_count, candidate_nodes.size()))

    struct NodeView {
        NodePtr node;
        int32 owned_replica_count{0};
        std::string dc;
    };

    std::vector<NodeView> views;
    views.reserve(candidate_nodes.size());
    for (const NodePtr& node : candidate_nodes) {
        views.push_back(
            NodeView{.node = node,
                     .owned_replica_count = node->derived.owned_replica_count,
                     .dc = node->spec.dc});
    }

    res.shards.clear();
    res.shards.reserve(param.shard_count);
    for (int32_t shard_idx = 0; shard_idx < param.shard_count; ++shard_idx) {
        std::sort(views.begin(), views.end(),
                  [](const NodeView& lhs, const NodeView& rhs) {
                      if (lhs.owned_replica_count != rhs.owned_replica_count) {
                          return lhs.owned_replica_count <
                                 rhs.owned_replica_count;
                      }
                      return lhs.node->id < rhs.node->id;
                  });

        ShardPlacement shard{
            .shard_index = shard_idx,
        };
        shard.nodes.reserve(param.replica_count);
        for (int32_t i = 0; i < param.replica_count; ++i) {
            shard.nodes.emplace_back(views[i].node);
            ++views[i].owned_replica_count;
        }
        res.shards.emplace_back(std::move(shard));
    }

    return Status::OK();
}

}  // namespace adviskv::sdm