#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/node_manager.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

// struct NodeSelectorParam{
//     std::vector<NodeStats> nodes;
//     int32_t shard_count;
//     int32_t replica_count;
// };

// 这个就是传进来一堆nodes，然后我们要选择出来replica_count个node，去放置replica。
// 所以这个selector是专门给一个shard分配的。 基本单位是shard
class NodeSelector {
   public:
    // virtual Status select_nodes(NodeSelectorParam param,
    // std::vector<std::vector<NodeID>>* res) const = 0; virtual Status
    // select_nodes(std::vector<NodePtr> param,
    // std::vector<std::vector<NodePtr>>& res) const = 0;
    virtual Status select_nodes(const std::vector<NodePtr>& nodes,
                                int32_t limit_count,
                                std::vector<NodePtr>& res) const = 0;
};

class DefaultNodeSelector : public NodeSelector {
   public:
    // Status select_nodes(NodeSelectorParam param,
    // std::vector<std::vector<NodeID>>* res) const override; Status
    // select_nodes(std::vector<NodePtr> param,
    // std::vector<std::vector<NodePtr>>& res) const;
    Status select_nodes(const std::vector<NodePtr>& nodes,
                        int32_t limit_count,
                        std::vector<NodePtr>& res) const override;

   private:
};

}  // namespace adviskv::sdm