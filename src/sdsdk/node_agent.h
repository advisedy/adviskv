#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"
#include "common/type.h"
#include "sdsdk/heartbeater.h"
#include "sdsdk/istorage_callback.h"
namespace adviskv::sdsdk {

struct NodeAgentConf {
    NodeID node_id;
    std::string ip;
    int32_t port;

    int64_t heartbeat_ts_ms;

    Status validate() {
        // TODO
        return Status::OK();
    }
};

class NodeAgent {
   public:
    NodeAgent() = default;
    Status init(NodeAgentConf conf, StorageCallbackPtr callback);
    Status start();
    Status stop();

   private:
    NodeAgentConf conf_;
    HeartBeaterPtr heartbeater_;
};

}  // namespace adviskv::sdsdk
