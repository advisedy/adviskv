#pragma once

#include <memory>
#include "common/background_task.h"
#include "sdsdk/istorage_callback.h"
#include "sdsdk/replica_controller.h"
namespace adviskv::sdsdk {

class HeartBeater : public BackgroundTask {
   public:

   HeartBeater(StorageCallbackPtr callback);
    void run() override;



    private:
    ReplicaController replica_controller_;
    StorageCallbackPtr callback_;
};

using HeartBeaterPtr = std::shared_ptr<HeartBeater>;
}  // namespace adviskv::sdsdk
