#pragma once

#include "common/status.h"
#include "common/type.h"
#include <cstdint>
#include <vector>

#include "sdm/manager/node_manager.h"

namespace adviskv{

struct NodeSelectorParam{
    std::vector<NodeStats> nodes;
    int32_t shard_count;
    int32_t replica_count;
};

class NodeSelector{
public:
    virtual Status select_nodes(NodeSelectorParam param, std::vector<std::vector<NodeID>>* res) const = 0;
};

class DefaultNodeSelector : public NodeSelector{
public:
    Status select_nodes(NodeSelectorParam param, std::vector<std::vector<NodeID>>* res) const override;
};

}