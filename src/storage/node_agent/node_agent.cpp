#include "storage/node_agent/node_agent.h"

#include <chrono>

#include "common/background_task.h"
#include "common/define.h"
#include "common/status.h"

namespace adviskv::storage {
Status NodeAgent::init(const NodeAgentConf& conf) {
    RETURN_IF_INVALID_STATUS(conf.validate())
    RETURN_IF_INVALID_CONDITION(!initialized_, "node agent already initialized")

    conf_ = conf;

    const std::string target =
        conf_.manager_host + ":" + std::to_string(conf_.manager_port);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = rpc::ShardingManagerService::NewStub(channel_);

    node_endpoint_ = {.ip = conf_.ip, .port = conf_.port};

    initialized_ = true;
    return Status::OK();
}

void NodeAgent::start() {
    RETURN_IF_INVALID_CONDITION(initialized_, "node agent not initialized")

    Status status = prepare();  // prepare环节会做好第一次连接
    RETURN_IF_INVALID_STATUS(status)

    BackgroundTask::start(
        std::chrono::milliseconds(conf_.heartbeat_interval_ms));

    return Status::OK();
}

Status NodeAgent::stop() {
    if (!initialized_) {
        return Status::OK();
    }
    BackgroundTask::stop();
    return Status::OK();
}

}  // namespace adviskv::storage
