#include "sdm/selector/node_selector/node_selector.h"

#include <fmt/format.h>

#include <algorithm>

#include "common/define.h"
#include "common/status.h"
#include "sdm/manager/node_manager.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

// class DefaultNodeSelector : public NodeSelector{
//     Status select_nodes(NodeSelectorParam param,
//     std::vector<std::vector<NodeID>>* res) const override;
// };

// Status DefaultNodeSelector::select_nodes(std::vector<NodePtr> param,
// std::vector<std::vector<NodePtr>>& res) const{
//     // 先简单实现一下，直接谁放的replica少谁就优先
// //     int32_t shard_count = param.shard_count;
// //     int32_t replica_count = param.replica_count;

// //     std::vector<NodeStats>& nodes = param.nodes;

// //   RETURN_IF_INVALID_CONDITION(
// //       nodes.size() >= replica_count,
// //       "node count should be greater than or equal to replica count")
// //   RETURN_IF_INVALID_CONDITION(res != nullptr,
// //                               "res should not be nullptr")

// //   for (int i = 0; i < shard_count; i++) {

// //     std::sort(nodes.begin(), nodes.end(),
// //               [](const NodeStats &a, const NodeStats &b) {
// //                 return a.owned_replica_count < b.owned_replica_count;
// //               });

// //     std::vector<NodeID> replica_nodes;
// //     replica_nodes.reserve(replica_count);
// //     for (int j = 0; j < replica_count; j++) {
// //       replica_nodes.emplace_back(nodes[j].node_id);
// //       nodes[j].owned_replica_count++;
// //     }
// //     res->emplace_back(std::move(replica_nodes));
// //   }

//   return Status::OK();

// }

Status DefaultNodeSelector::select_nodes(const std::vector<NodePtr>& nodes,
                                         int32_t limit_count,
                                         std::vector<NodePtr>& res) const {
    RETURN_IF_INVALID_CONDITION(
        nodes.size() >= limit_count,
        fmt::format("node size:{} should >= limit_count:{}", nodes.size(),
        limit_count));

    std::vector<NodePtr> candi_nodes = nodes;

    // TODO 这里要提前晒一下，传进来的nodes是否有空指针

    std::sort(candi_nodes.begin(), candi_nodes.end(),
              [](const NodePtr& q1, const NodePtr& q2) {
                  return q1->state.owned_replica_count <
                         q2->state.owned_replica_count;
              });

    res.clear();
    res.reserve(limit_count);
    for (int i = 0; i < limit_count; i++) {
        res.emplace_back(candi_nodes[i]);
    }

    return Status::OK();
}

}  // namespace adviskv::sdm