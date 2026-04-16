#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/node_manager.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

class LeaderSelector {
   public:
    // virtual Status select_leader(const std::vector<NodeStats>& nodes, NodeID&
    // leader_node_id) const = 0;
    virtual Status select_leader(const std::vector<NodePtr>& nodes,
                                 NodePtr& leader) const = 0;
};

class DefaultLeaderSelector : public LeaderSelector {
   public:
    //    Status select_leader(const std::vector<NodeStats>& nodes, NodeID&
    //    leader) const override;
    Status select_leader(const std::vector<NodePtr>& nodes,
                         NodePtr& leader) const override;
};

}  // namespace adviskv::sdm