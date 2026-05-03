#pragma once

#include <grpcpp/channel.h>

#include <memory>

#include "common/background_task.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm.grpc.pb.h"
#include "storage/replica/replica_manager.h"

namespace adviskv::storage {


struct NodeAgentConf {
    NodeID node_id;
    std::string ip;
    int32_t port{-1};

    std::string resource_pool{"default"};
    std::string dc;

    std::string manager_host;
    int32_t manager_port{-1};

    int32_t heartbeat_interval_ms{3000};
    int32_t first_sync_retry_ms{1000};
    int32_t action_worker_count{4};

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
        RETURN_IF_INVALID_CONDITION(first_sync_retry_ms > 0,
                                    "first_sync_retry_ms should > 0")
        RETURN_IF_INVALID_CONDITION(action_worker_count > 0,
                                    "action_worker_count should > 0")
        return Status::OK();
    }
};

class NodeAgent : public BackgroundTask {
   public:
    NodeAgent() = default;
    Status init(const NodeAgentConf& conf);
    Status start();
    Status stop();

    void run() override;

   protected:
    Status setup() override;

   private:
    Status heartbeat_once();
    Status register_node();

    NodeAgentConf conf_;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<rpc::ShardingManagerService::Stub> stub_;
    std::shared_ptr<ReplicaManager> replica_manager_;

    Endpoint node_endpoint_;
    bool initialized_{false};
};

}  // namespace adviskv::storage
