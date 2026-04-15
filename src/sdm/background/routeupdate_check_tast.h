#pragma once

#include "common/type.h"
#include "sdm/background/background_task.h"
#include "sdm/model/route_model.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/leader_selector/leader_selector.h"
#include "sdm/selector/node_selector/node_selector.h"
namespace adviskv::sdm{

// 这个的职责是显示
class RouteUpdateCheckTask :public BackgroundTask{
protected:
    void run() override;

private:

    Status check_shard_route(const Table& table, ShardID shard_id);


    SdmStore sdm_store_;
    DefaultLeaderSelector leader_selector_;
    // DefaultNodeSelector node_selector_;
};

}