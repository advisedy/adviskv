#pragma once

#include "common/background_task.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

class HeartBeatCheckTask : public BackgroundTask {
   public:
    explicit HeartBeatCheckTask(SdmStore* sdm_store);

   protected:
    void run() override;

   private:
    Status check_and_modify_node(Node& node);

    Status mark_node_offline(Node& node);

    SdmStore* sdm_store_;
};

}  // namespace adviskv::sdm