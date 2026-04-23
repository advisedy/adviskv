#include "sdsdk/heartbeater.h"

#include <chrono>
#include <thread>

#include "common/define.h"
#include "common/log.h"

namespace adviskv::sdsdk {

HeartBeater::HeartBeater(const NodeAgentConf& conf, StorageCallbackPtr callback)
    : conf_(conf), callback_(std::move(callback)) {}

Status HeartBeater::init() {
    RETURN_IF_INVALID_CONDITION(!initialized_,
                                "heartbeater already initialized")
    RETURN_IF_INVALID_CONDITION(callback_ != nullptr, "callback is nullptr")

    const std::string target =
        conf_.manager_host + ":" + std::to_string(conf_.manager_port);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = rpc::ShardingManagerService::NewStub(channel_);

    Status status =
        replica_controller_.init(callback_, conf_.action_worker_count);
    RETURN_IF_INVALID_STATUS(status)

    initialized_ = true;
    return Status::OK();
}

Status HeartBeater::setup() {
    RETURN_IF_INVALID_CONDITION(initialized_, "heartbeater not initialized")

    Status status = register_node();
    RETURN_IF_INVALID_STATUS(status)

    while (true) {
        status = heartbeat_once();
        if (status.ok()) {
            first_sync_finished_ = true;
            return Status::OK();
        }
        WARN("sdsdk first heartbeat failed, msg={}", status.to_string());
        std::this_thread::sleep_for(
            std::chrono::milliseconds(conf_.first_sync_retry_ms));
    }
    return Status::OK();
}

Status HeartBeater::stop_and_wait() {
    stop();
    replica_controller_.stop();
    return Status::OK();
}

void HeartBeater::run() {
    Status status = heartbeat_once();
    if (!status.ok()) {
        WARN("sdsdk heartbeat failed, msg={}", status.to_string());
    }
}

Status HeartBeater::heartbeat_once() {
    NodeReport node_report;
    Status status = callback_->collect_node_report(node_report);
    RETURN_IF_INVALID_STATUS(status)

    if (node_report.endpoint.ip.empty()) {
        node_report.endpoint.ip = conf_.ip;
        node_report.endpoint.port = conf_.port;
    }

    std::vector<ReplicaReport> replica_reports;
    status =
        replica_controller_.collect_cached_replica_reports(replica_reports);
    RETURN_IF_INVALID_STATUS(status)

    rpc::HeartBeatRequest request = make_request(node_report, replica_reports);
    rpc::HeartBeatResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status = stub_->HeartBeat(&context, request, &response);
    RETURN_IF_INVALID_CONDITION(grpc_status.ok(), grpc_status.error_message())
    RETURN_IF_INVALID_CONDITION(response.base_rsp().code() == 0,
                                response.base_rsp().msg())

    std::vector<DesiredReplicaSpec> desired_set = parse_desired_set(response);
    status = replica_controller_.apply_desired_set(desired_set);
    return status;
}

Status HeartBeater::register_node() {
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

rpc::HeartBeatRequest HeartBeater::make_request(
    const NodeReport& node_report,
    const std::vector<ReplicaReport>& replicas) const {
    rpc::HeartBeatRequest request;
    request.set_node_id(conf_.node_id);
    request.set_ip(node_report.endpoint.ip);
    request.set_port(node_report.endpoint.port);
    request.set_resource_pool(conf_.resource_pool);
    request.set_dc(conf_.dc);

    for (const ReplicaReport& replica : replicas) {
        auto* info = request.add_replica_info_list();
        info->set_table_id(replica.key.table_id);
        info->set_shard_id(replica.key.shard_index);
        info->set_replica_index(replica.key.replica_index);
        info->set_role(replica.role);
        info->set_status(replica.status);
    }
    return request;
}

std::vector<DesiredReplicaSpec> HeartBeater::parse_desired_set(
    const rpc::HeartBeatResponse& response) const {
    std::vector<DesiredReplicaSpec> desired_set;
    desired_set.reserve(response.replica_list_size());

    for (const auto& replica : response.replica_list()) {
        DesiredReplicaSpec spec{
            .key.table_id = replica.table_id(),
            .key.shard_index = replica.shard_id(),
            .key.replica_index = replica.replica_index(),
            .role = replica.role(),
        };
        desired_set.push_back(std::move(spec));
    }
    return desired_set;
}

}  // namespace adviskv::sdsdk
