#pragma once

#include "common/type.h"
#include "common/background_task.h"

#include "sdm/model/sdm_store.h"
#include "sdm/selector/leader_selector/leader_selector.h"
#include "sdm/selector/node_selector/node_selector.h"
namespace adviskv::sdm {

// 这个的职责是更新路由表（从sdm_store里面的replica给更新到路由表上），同时确认好这个路由表上的replica的leader和follower
class RouteUpdateCheckTask : public BackgroundTask {
   protected:
    void run() override;

   private:
    Status check_shard_route(const Table& table, ShardID shard_id);

    SdmStore* sdm_store_;
    DefaultLeaderSelector leader_selector_;
    // DefaultNodeSelector node_selector_;
};

}  // namespace adviskv::sdm