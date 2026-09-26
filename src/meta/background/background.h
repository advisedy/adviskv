#pragma once

#include "common/background_task.h"

namespace adviskv::meta {

class ServiceManager;

// Meta 的唯一后台收敛任务：每轮调用 ServiceManager::tick()，
// 按 nodes -> replica_groups -> routes -> tables 顺序推进一轮。
// DDL 与 heartbeat 仍可并发进入，各阶段须幂等重入。
class MetaReconcileTask : public BackgroundTask {
public:
    explicit MetaReconcileTask(ServiceManager* service_manager);

    void reconcile_once();

protected:
    void run() override;

private:
    ServiceManager* service_manager_{nullptr};
};

}  // namespace adviskv::meta
