#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
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
// update: 现在是直接对于一个table去分配了.
class NodeSelector {
   public:
    virtual ~NodeSelector() = default;

    virtual Status select_table_nodes(const PlaceNodesParam& param,
                                      TablePlacementResult& res) const = 0;
};

class DefaultNodeSelector : public NodeSelector {
   public:
    explicit DefaultNodeSelector(SdmStore* store) : store_(store) {}

    Status select_table_nodes(const PlaceNodesParam& param,
                              TablePlacementResult& res) const override;

   private:
    SdmStore* store_;
};

}  // namespace adviskv::sdm