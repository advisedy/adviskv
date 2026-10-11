#pragma once
#include "common/background_task.h"
#include "common/model/model.h"

namespace adviskv::storage {

class ReplicaManager;

class RaftTickTask : public BackgroundTask {
public:
    explicit RaftTickTask(ReplicaManager* manager);

    ~RaftTickTask() override { stop(); }

    void run() override;

private:
    ReplicaManager* manager_;
};

}  // namespace adviskv::storage