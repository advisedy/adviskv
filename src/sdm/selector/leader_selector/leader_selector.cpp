#include "sdm/selector/leader_selector/leader_selector.h"

#include "common/define.h"
#include "common/status.h"
#include "sdm/manager/node_manager.h"

namespace adviskv::sdm {

// Status DefaultLeaderSelector::select_leader(const std::vector<NodeStats>&
// nodes, NodeID& leader) const {
//     // 先简单实现一下，直接第一个就是leader

//     RETURN_IF_INVALID_CONDITION(!nodes.empty(), "nodes should not be empty")

//     leader = nodes[0].node_id;

//   return Status::OK();

// }

Status DefaultLeaderSelector::select_leader(const std::vector<NodePtr>& nodes,
                                            NodePtr& leader) const {
    RETURN_IF_INVALID_CONDITION(!nodes.empty(), "nodes should not be empty")
    leader = nodes[0];
    return Status::OK();
}

}  // namespace adviskv::sdm