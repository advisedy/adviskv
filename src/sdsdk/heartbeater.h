#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>

#include "common/background_task.h"
#include "sdsdk/istorage_callback.h"
#include "sdsdk/replica_controller.h"
#include "sdsdk/type.h"
#include "sdm.grpc.pb.h"

namespace adviskv::sdsdk {

class HeartBeater : public BackgroundTask {
   public:
    HeartBeater(const NodeAgentConf& conf, StorageCallbackPtr callback);

    Status init();
    Status stop_and_wait();

    void run() override;

   protected:
    Status setup() override;

   private:
    Status heartbeat_once();
    Status register_node();

    rpc::HeartBeatRequest make_request(
        const std::vector<ReplicaReport>& replicas) const;

    std::vector<DesiredReplicaSpec> parse_desired_set(
        const rpc::HeartBeatResponse& response) const;

   private:
    NodeAgentConf conf_;
    ReplicaController replica_controller_;
    StorageCallbackPtr callback_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<rpc::ShardingManagerService::Stub> stub_;
    bool initialized_{false};
    bool first_sync_finished_{false};
};

using HeartBeaterPtr = std::shared_ptr<HeartBeater>;
}  // namespace adviskv::sdsdk
