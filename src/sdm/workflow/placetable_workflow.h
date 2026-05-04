#pragma once

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/client/storage_client.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"
#include "sdm/selector/node_selector/node_selector.h"

namespace adviskv::sdm {

class PlaceTableWorkflow {
   public:
    PlaceTableWorkflow(SdmStore* store, StorageClient* client,
                       NodeSelector* selector);

    Status step(Table& table);

   private:
    Status step_creating(Table& table);
    Status step_placing(Table& table);
    Status step_creating_replicas(Table& table);

    // 等待storage那边创建完replica，这一块是异步的。
    Status step_waiting_ready(Table& table);
    // 路由表由后台任务维护，这里只等待它准备好。
    Status step_waiting_route_ready(Table& table);
    Status step_rolling_back(Table& table);

    Status transition(Table& table, TableLifecycle next,
                      const std::string& error_msg = "");

    SdmStore* store_;
    StorageClient* client_;
    NodeSelector* selector_;
};

}  // namespace adviskv::sdm
