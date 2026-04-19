

#include "sdsdk/node_agent.h"

#include <memory>
#include <stdexcept>

#include "common/define.h"
#include "sdsdk/heartbeater.h"

namespace adviskv::sdsdk {



Status NodeAgent::init(NodeAgentConf conf, StorageCallbackPtr callback) {
    RETURN_IF_INVALID_CONDITION(!callback, "callback is nullptr");
    RETURN_IF_INVALID_CONF(conf)
    conf_ = conf;

    heartbeater_ = std::make_shared<HeartBeater>(std::move(callback));
}

}  // namespace adviskv::sdsdk