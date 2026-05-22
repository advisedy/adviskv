#pragma once

#include "common/background_task.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

class HeartBeatCheckTask : public BackgroundTask {
   public:
    static constexpr int64_t SUSPECT_TIMEOUT_MS = 10 * 1000;
    static constexpr int64_t OFFLINE_TIMEOUT_MS = 30 * 1000;
    static constexpr int64_t STARTUP_GRACE_MS = 30 * 1000;

    explicit HeartBeatCheckTask(SdmStore* sdm_store);

   protected:
    void run() override;

   private:
    Status check_and_modify_node(Node& node);

    Status mark_node_offline(Node& node);

    Status mark_node_suspect(Node& node);

    Status mark_node_online(Node& node);

    SdmStore* sdm_store_;
    int64_t start_ts_ms_;
};

}  // namespace adviskv::sdm