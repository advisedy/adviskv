#pragma once

#include "common/status.h"
#include "common/type.h"
#include <cstdint>
#include <vector>

#include "sdm/manager/node_manager.h"

namespace adviskv{


class LeaderSelector{
public:
    virtual Status select_leader(const std::vector<NodeStats>& nodes, NodeID& leader_node_id) const = 0;
};

class DefaultLeaderSelector : public LeaderSelector{
public:
   Status select_leader(const std::vector<NodeStats>& nodes, NodeID& leader) const override;
};

}