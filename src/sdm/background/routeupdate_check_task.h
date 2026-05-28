#pragma once

#include "common/type.h"
#include "common/background_task.h"

#include "sdm/model/sdm_store.h"
#include "sdm/selector/leader_selector/leader_selector.h"
#include "sdm/selector/node_selector/node_selector.h"
namespace adviskv::sdm {

// 这个的职责是更新路由表（从sdm_store里面的replica给更新到路由表上），同时确认好这个路由表上的replica的leader和follower
class RouteUpdateCheckTask : public BackgroundTask {
   public:
    explicit RouteUpdateCheckTask(SdmStore* sdm_store) : sdm_store_(sdm_store) {}
    Status update_once();
    
    protected:
    void run() override;
    

    Status check_shard_route(const Table& table, ShardIndex shard_index);

    SdmStore* sdm_store_;
};

}  // namespace adviskv::sdm
