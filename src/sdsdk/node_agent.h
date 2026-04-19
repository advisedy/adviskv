#pragma once

#include "common/status.h"
#include "sdsdk/istorage_callback.h"
#include "sdsdk/type.h"

namespace adviskv::sdsdk {

class HeartBeater;
using HeartBeaterPtr = std::shared_ptr<HeartBeater>;


class NodeAgent {
   public:
    NodeAgent() = default;
    Status init(const NodeAgentConf& conf, StorageCallbackPtr callback);
    Status start();
    Status stop();

   private:
    NodeAgentConf conf_;
    StorageCallbackPtr callback_;
    HeartBeaterPtr heartbeater_;
    bool initialized_{false};
};

}  // namespace adviskv::sdsdk
