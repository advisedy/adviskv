#pragma once

#include <grpcpp/channel.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/background_task.h"
#include "common/define.h"
#include "common/model/expected_replica.h"
#include "common/proto/rpc_alias.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm.grpc.pb.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

struct NodeAgentReplicaOps {
    std::function<std::vector<ReplicaPtr>()> list_replicas;
    std::function<Status(const ReplicaInitParam&)> create_replica;
    std::function<Status(const ReplicaID&)> delete_replica;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(to<bool>(list_replicas),
                                    "list_replicas callback is empty")
        RETURN_IF_INVALID_CONDITION(to<bool>(create_replica),
                                    "create_replica callback is empty")
        RETURN_IF_INVALID_CONDITION(to<bool>(delete_replica),
                                    "delete_replica callback is empty")
        return Status::OK();
    }
};

struct NodeAgentConf {
    NodeID node_id;
    std::string ip;
    int32_t port{-1};

    std::string resource_pool{"default"};
    std::string dc;

    std::string manager_host;
    int32_t manager_port{-1};

    int32_t heartbeat_interval_ms{3000};
    int32_t register_interval_ms{30 * 1000};
    int32_t first_sync_retry_ms{1000};
    NodeAgentReplicaOps replica_ops;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!node_id.empty(),
                                    "node_id should not empty")
        RETURN_IF_INVALID_CONDITION(!ip.empty(), "ip should not empty")
        RETURN_IF_INVALID_CONDITION(port > 0, "port should > 0")
        RETURN_IF_INVALID_CONDITION(!resource_pool.empty(),
                                    "resource_pool should not empty")
        RETURN_IF_INVALID_CONDITION(!dc.empty(), "dc should not empty")
        RETURN_IF_INVALID_CONDITION(!manager_host.empty(),
                                    "manager_host should not empty")
        RETURN_IF_INVALID_CONDITION(manager_port > 0, "manager_port should > 0")
        RETURN_IF_INVALID_CONDITION(heartbeat_interval_ms > 0,
                                    "heartbeat_interval_ms should > 0")
        RETURN_IF_INVALID_CONDITION(register_interval_ms > 0,
                                    "register_interval_ms should > 0")
        RETURN_IF_INVALID_CONDITION(first_sync_retry_ms > 0,
                                    "first_sync_retry_ms should > 0")
        RETURN_IF_INVALID_STATUS(replica_ops.validate())
        return Status::OK();
    }
};

class NodeAgent {
   public:
    NodeAgent();
    ~NodeAgent();

    Status init(NodeAgentConf conf);
    Status start();
    Status stop();

   private:
    class HeartbeatTask;
    class RegisterTask;

    Status heartbeat_once();
    Status send_heartbeat_once(sdm_rpc::HeartbeatResponse* response);

    Status register_node();

    Status apply_expected_replica(const ExpectedReplica& instruction);
    ReplicaInitParam make_replica_init_param(
        const ExpectedReplica& expects) const;
    sdm_rpc::HeartbeatRequest make_heartbeat_request() const;

    NodeAgentConf conf_;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<sdm_rpc::SdmService::Stub> stub_;
    std::unique_ptr<HeartbeatTask> heartbeat_task_;
    std::unique_ptr<RegisterTask> register_task_;
    bool initialized_{false};
};

}  // namespace adviskv::storage
