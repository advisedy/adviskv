#pragma once

#include "common/background_task.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/node_selector/node_selector.h"
namespace adviskv::sdm {

// 这个就是把sdm_store里面的那些replica给分配好去哪一个node
// 至于之后的内容，就是交给route_update_task去做的
class ReplicaScheduleTask : public BackgroundTask {
   protected:
    void run() override;

   private:
    Status check_shard(const Table& table, ShardIndex shard_index);
    SdmStore* sdm_store_;
    DefaultNodeSelector node_selector_;
};

}  // namespace adviskv::sdm
