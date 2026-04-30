#pragma once
#include "common/background_task.h"
#include "common/type.h"

namespace adviskv::storage {

class ReplicaManager;

class RaftTickTask : public BackgroundTask {
   public:
    explicit RaftTickTask(ReplicaManager* manager);

    void run() override;

   private:
    ReplicaManager* manager_;
};

}  // namespace adviskv::storage