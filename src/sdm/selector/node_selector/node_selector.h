#pragma once

#include "common/status.h"
#include "common/type.h"
#include <cstdint>
#include <vector>

#include "sdm/manager/node_manager.h"
#include "sdm/model/store.h"

namespace adviskv::sdm{

// struct NodeSelectorParam{
//     std::vector<NodeStats> nodes;
//     int32_t shard_count;
//     int32_t replica_count;
// };

class NodeSelector{
public:
    // virtual Status select_nodes(NodeSelectorParam param, std::vector<std::vector<NodeID>>* res) const = 0;
    virtual Status select_nodes(std::vector<NodePtr> param, std::vector<std::vector<NodePtr>>& res) const = 0;
};

class DefaultNodeSelector : public NodeSelector{
public:
    // Status select_nodes(NodeSelectorParam param, std::vector<std::vector<NodeID>>* res) const override;
    Status select_nodes(std::vector<NodePtr> param, std::vector<std::vector<NodePtr>>& res) const;

};

}