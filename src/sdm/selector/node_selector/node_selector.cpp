#include "sdm/selector/node_selector/node_selector.h"
#include "common/status.h"
#include "sdm/manager/node_manager.h"
#include "common/define.h"

namespace adviskv{

// class DefaultNodeSelector : public NodeSelector{
//     Status select_nodes(NodeSelectorParam param, std::vector<std::vector<NodeID>>* res) const override;
// };

Status DefaultNodeSelector::select_nodes(NodeSelectorParam param, std::vector<std::vector<NodeID>>* res) const{
    // 先简单实现一下，直接谁放的replica少谁就优先
    int32_t shard_count = param.shard_count;
    int32_t replica_count = param.replica_count;

    std::vector<NodeStats>& nodes = param.nodes;

  RETURN_IF_INVALID_CONDITION(
      nodes.size() >= replica_count,
      "node count should be greater than or equal to replica count")
  RETURN_IF_INVALID_CONDITION(res != nullptr,
                              "res should not be nullptr")

  for (int i = 0; i < shard_count; i++) {

    std::sort(nodes.begin(), nodes.end(),
              [](const NodeStats &a, const NodeStats &b) {
                return a.owned_replica_count < b.owned_replica_count;
              });

    std::vector<NodeID> replica_nodes;
    replica_nodes.reserve(replica_count);
    for (int j = 0; j < replica_count; j++) {
      replica_nodes.emplace_back(nodes[j].node_id);
      nodes[j].owned_replica_count++;
    }
    res->emplace_back(std::move(replica_nodes));
  }

  return Status::OK();



}

}