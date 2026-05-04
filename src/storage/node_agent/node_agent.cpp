#include "storage/node_agent/node_agent.h"

#include <chrono>
#include <thread>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"

namespace adviskv::storage {
namespace {

pb::ReplicaRole to_pb_replica_role(ReplicaRole role) {
    switch (role) {
        case ReplicaRole::LEADER:
            return pb::ReplicaRole::LEADER;
        case ReplicaRole::FOLLOWER:
        case ReplicaRole::CANDIDATE:
            // SDM V1 only consumes leader/follower observations.
            return pb::ReplicaRole::FOLLOWER;
    }
    return pb::ReplicaRole::FOLLOWER;
}

pb::ReplicaStatus to_pb_replica_status(ReplicaStatus status) {
    switch (status) {
        case ReplicaStatus::ADDING:
            return pb::ReplicaStatus::ADDING;
        case ReplicaStatus::READY:
            return pb::ReplicaStatus::READY;
        case ReplicaStatus::LOST:
            return pb::ReplicaStatus::LOST;
        case ReplicaStatus::ERROR:
            return pb::ReplicaStatus::ERROR;
    }
    return pb::ReplicaStatus::ERROR;
}

}  // namespace

Status NodeAgent::init(const NodeAgentConf& conf,
                       ReplicaManager* replica_manager) {
    RETURN_IF_INVALID_STATUS(conf.validate())
    RETURN_IF_INVALID_CONDITION(!initialized_, "node agent already initialized")
    RETURN_IF_INVALID_CONDITION(replica_manager != nullptr,
                                "replica_manager is nullptr")

    conf_ = conf;
    replica_manager_ = replica_manager;

    const std::string target =
        conf_.manager_host + ":" + std::to_string(conf_.manager_port);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = rpc::ShardingManagerService::NewStub(channel_);

    initialized_ = true;
    return Status::OK();
}

Status NodeAgent::start() {
    RETURN_IF_INVALID_CONDITION(initialized_, "node agent not initialized")

    Status status = prepare();
    RETURN_IF_INVALID_STATUS(status)

    BackgroundTask::start(
        Milliseconds(conf_.heartbeat_interval_ms));

    return Status::OK();
}

Status NodeAgent::stop() {
    if (!initialized_) {
        return Status::OK();
    }
    BackgroundTask::stop();
    return Status::OK();
}

Status NodeAgent::setup() {
    RETURN_IF_INVALID_CONDITION(initialized_, "node agent not initialized")

    Status status = register_node();
    RETURN_IF_INVALID_STATUS(status)

    while (true) {
        status = heartbeat_once();
        if (status.ok()) {
            return Status::OK();
        }
        LOG_WARN("node agent first heartbeat failed, msg={}", status.msg());
        std::this_thread::sleep_for(
            Milliseconds(conf_.first_sync_retry_ms));
    }
    return Status::OK();
}

void NodeAgent::run() {
    Status status = heartbeat_once();
    if (!status.ok()) {
        LOG_WARN("node agent heartbeat failed, msg={}", status.msg());
    }
}

Status NodeAgent::heartbeat_once() {
    rpc::HeartBeatRequest request = make_heartbeat_request();
    rpc::HeartBeatResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status = stub_->HeartBeat(&context, request, &response);
    RETURN_IF_INVALID_CONDITION(grpc_status.ok(), grpc_status.error_message())
    RETURN_IF_INVALID_CONDITION(response.base_rsp().code() == 0,
                                response.base_rsp().msg())
    return Status::OK();
}

Status NodeAgent::register_node() {
    rpc::RegisterNodeRequest request;
    request.set_node_id(conf_.node_id);
    request.set_ip(conf_.ip);
    request.set_port(conf_.port);
    request.set_resource_pool(conf_.resource_pool);
    request.set_dc(conf_.dc);

    rpc::RegisterNodeResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status =
        stub_->RegisterNode(&context, request, &response);
    RETURN_IF_INVALID_CONDITION(grpc_status.ok(), grpc_status.error_message())
    RETURN_IF_INVALID_CONDITION(response.base_rsp().code() == 0,
                                response.base_rsp().msg())
    return Status::OK();
}

rpc::HeartBeatRequest NodeAgent::make_heartbeat_request() const {
    rpc::HeartBeatRequest request;
    request.set_node_id(conf_.node_id);
    request.set_ip(conf_.ip);
    request.set_port(conf_.port);
    request.set_resource_pool(conf_.resource_pool);
    request.set_dc(conf_.dc);

    for (Replica* replica : replica_manager_->get_replicas()) {
        if (replica == nullptr) {
            continue;
        }
        auto* info = request.add_replica_info_list();
        const ReplicaID replica_id = replica->get_replica_id();
        info->set_table_id(replica_id.table_id);
        info->set_shard_id(replica_id.shard_index);
        info->set_replica_index(replica_id.replica_index);
        info->set_role(to_pb_replica_role(replica->get_role()));
        info->set_status(to_pb_replica_status(replica->get_status()));
    }
    return request;
}

}  // namespace adviskv::storage
