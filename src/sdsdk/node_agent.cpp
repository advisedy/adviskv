#include "sdsdk/node_agent.h"

#include <chrono>

#include "common/define.h"
#include "sdsdk/heartbeater.h"

namespace adviskv::sdsdk {
Status NodeAgent::init(const NodeAgentConf& conf, StorageCallbackPtr callback) {
    RETURN_IF_INVALID_CONDITION(callback != nullptr, "callback is nullptr")
    RETURN_IF_INVALID_STATUS(conf.validate())
    RETURN_IF_INVALID_CONDITION(!initialized_, "node agent already initialized")

    conf_ = conf;
    callback_ = std::move(callback);
    heartbeater_ = std::make_shared<HeartBeater>(conf_, callback_);
    Status status = heartbeater_->init();
    RETURN_IF_INVALID_STATUS(status)

    initialized_ = true;
    return Status::OK();
}

Status NodeAgent::start() {
    RETURN_IF_INVALID_CONDITION(initialized_, "node agent not initialized")
    RETURN_IF_INVALID_CONDITION(heartbeater_ != nullptr, "heartbeater is nullptr")

    Status status = heartbeater_->prepare(); // prepare环节会做好第一次连接
    RETURN_IF_INVALID_STATUS(status)

    heartbeater_->start(std::chrono::milliseconds(conf_.heartbeat_interval_ms));
    return Status::OK();
}

Status NodeAgent::stop() {
    if (!initialized_ || heartbeater_ == nullptr) {
        return Status::OK();
    }
    return heartbeater_->stop_and_wait();
}

}  // namespace adviskv::sdsdk
