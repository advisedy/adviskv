#include "storage/node_agent/node_agent.h"

#include <fmt/format.h>
#include <grpcpp/create_channel.h>

#include <chrono>
#include <memory>
#include <thread>
#include <utility>

#include "common/define.h"
#include "common/log.h"
#include "common/proto/expected_replica_proto.h"
#include "common/proto/raft_member_type_proto.h"
#include "common/proto/raft_role_proto.h"
#include "common/proto/replica_id_proto.h"
#include "common/proto/storage_replica_status_proto.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

class NodeAgent::HeartbeatTask : public BackgroundTask {
   public:
    HeartbeatTask(NodeAgent* agent, Milliseconds interval)
        : agent_(agent), interval_(interval) {}

    Milliseconds interval() const { return interval_; }

   private:
    void run() override {
        Status status = agent_->heartbeat_once();
        if (!status.ok()) {
            LOG_WARN("[NodeAgent] node agent heartbeat failed, msg={}",
                     status.msg());
        }
    }

    NodeAgent* agent_{nullptr};
    Milliseconds interval_{0};
};

class NodeAgent::RegisterTask : public BackgroundTask {
   public:
    RegisterTask(NodeAgent* agent, Milliseconds first_retry_interval,
                 Milliseconds interval)
        : agent_(agent),
          first_retry_interval_(first_retry_interval),
          interval_(interval) {}

    Milliseconds interval() const { return interval_; }

    Status setup() override {
        while (true) {
            Status status = agent_->register_node();
            if (status.ok()) {
                return Status::OK();
            }
            LOG_WARN(
                "[NodeAgent] node agent initial register failed, status={}",
                status.to_string());
            std::this_thread::sleep_for(first_retry_interval_);
        }
    }

   private:
    void run() override {
        Status status = agent_->register_node();
        if (!status.ok()) {
            LOG_WARN("node agent register failed, msg={}", status.msg());
        }
    }

    NodeAgent* agent_{nullptr};
    Milliseconds first_retry_interval_{0};
    Milliseconds interval_{0};
};

NodeAgent::NodeAgent() = default;

NodeAgent::~NodeAgent() { stop(); }

Status NodeAgent::init(NodeAgentConf conf) {
    RETURN_IF_INVALID_STATUS(conf.validate())
    RETURN_IF_INVALID_CONDITION(!initialized_, "node agent already initialized")

    conf_ = std::move(conf);

    const std::string target =
        conf_.manager_host + ":" + std::to_string(conf_.manager_port);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = rpc::ShardingManagerService::NewStub(channel_);
    register_task_ = std::make_unique<RegisterTask>(
        this, Milliseconds(conf_.first_sync_retry_ms),
        Milliseconds(conf_.register_interval_ms));
    heartbeat_task_ = std::make_unique<HeartbeatTask>(
        this, Milliseconds(conf_.heartbeat_interval_ms));

    initialized_ = true;
    return Status::OK();
}

Status NodeAgent::start() {
    RETURN_IF_INVALID_CONDITION(initialized_, "node agent not initialized")

    RETURN_IF_INVALID_STATUS(register_task_->setup())
    register_task_->start(register_task_->interval());

    RETURN_IF_INVALID_STATUS(heartbeat_task_->setup())
    heartbeat_task_->start(heartbeat_task_->interval());

    return Status::OK();
}

Status NodeAgent::stop() {
    if (!initialized_) {
        return Status::OK();
    }
    if (heartbeat_task_) {
        heartbeat_task_->stop();
    }
    if (register_task_) {
        register_task_->stop();
    }
    return Status::OK();
}

Status NodeAgent::heartbeat_once() {
    rpc::HeartBeatResponse response;
    Status status = send_heartbeat_once(&response);
    RETURN_IF_INVALID_STATUS(status)

    for (const auto& expected_pb : response.expects()) {
        ExpectedReplica instruction = decode_pb_expected_replica(expected_pb);
        Status apply_status = apply_expected_replica(instruction);
        if (apply_status.fail()) {
            LOG_WARN(
                "[NodeAgent] node agent apply expected replica failed, msg={}",
                apply_status.msg());
        }
    }
    return Status::OK();
}

Status NodeAgent::send_heartbeat_once(rpc::HeartBeatResponse* response) {
    RETURN_IF_NULLPTR(response, "heartbeat response is nullptr")

    rpc::HeartBeatRequest request = make_heartbeat_request();
    grpc::ClientContext context;
    grpc::Status grpc_status = stub_->HeartBeat(&context, request, response);
    RETURN_IF_INVALID_CONDITION(grpc_status.ok(), grpc_status.error_message())
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response->base_rsp()))
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
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

Status NodeAgent::apply_expected_replica(const ExpectedReplica& expect) {
    ReplicaID replica_id = expect.replica_id;
    switch (expect.type) {
        case ExpectedReplicaType::PRESENT: {
            ReplicaInitParam param = make_replica_init_param(expect);
            Status status = conf_.replica_ops.create_replica(param);
            if (status.fail()) {
                return Status::ERROR(
                    fmt::format("create replica {} failed: status:{}",
                                replica_id.to_string(), status.to_string()));
            }
            return Status::OK();
        }
        case ExpectedReplicaType::ABSENT: {
            Status status = conf_.replica_ops.delete_replica(replica_id);
            if (status.fail()) {
                return Status::ERROR(
                    fmt::format("delete replica {} failed: status:{}",
                                replica_id.to_string(), status.to_string()));
            }
            return Status::OK();
        }
        case ExpectedReplicaType::ADD_MEMBER:
        case ExpectedReplicaType::REMOVE_MEMBER:
            return Status::ERROR(
                fmt::format("expected replica type {} is not supported yet",
                            to<int8>(expect.type)));
        default:
            return Status::ERROR(fmt::format("invalid expected replica type {}",
                                             to<int8>(expect.type)));
    }
}

ReplicaInitParam NodeAgent::make_replica_init_param(
    const ExpectedReplica& expect) const {
    ReplicaInitParam param;
    param.replica_id = expect.replica_id;
    param.engine_type = expect.engine_type;
    param.local_endpoint = Endpoint{conf_.ip, conf_.port};

    for (const PeerMember& member : expect.initial_members) {
        param.members.push_back(member);
    }
    return param;
}

rpc::HeartBeatRequest NodeAgent::make_heartbeat_request() const {
    rpc::HeartBeatRequest request;
    request.set_node_id(conf_.node_id);
    request.set_ip(conf_.ip);
    request.set_port(conf_.port);
    request.set_resource_pool(conf_.resource_pool);
    request.set_dc(conf_.dc);

    std::vector<ReplicaPtr>&& replicas = conf_.replica_ops.list_replicas();
    for (ReplicaPtr& replica : replicas) {
        if (replica == nullptr) {
            continue;
        }
        auto* info = request.add_replica_info_list();
        const ReplicaID replica_id = replica->get_replica_id();
        encode_pb_replica_id(replica_id, *info->mutable_replica_id());
        info->set_role(to_pb_raft_role(replica->get_role()));
        info->set_status(to_pb_storage_replica_status(replica->get_status()));
        info->set_term(replica->current_term());
        info->set_member_type(
            to_pb_raft_member_type(replica->get_member_type()));
    }
    return request;
}

}  // namespace adviskv::storage