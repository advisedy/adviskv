#pragma once

#include <grpcpp/channel.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/type.h"
#include "storage.grpc.pb.h"

namespace adviskv::sdm {

class StorageClient {
   public:
    StorageClient() = default;

    Status create_replica(const std::string& ip, int32_t port,
                          const rpc::CreateReplicaRequest& request,
                          rpc::CreateReplicaResponse& response);

   private:
    rpc::StorageService::Stub* get_or_create_stub(const std::string& ip,
                                                  int32_t port);

    std::unordered_map<std::string, std::unique_ptr<rpc::StorageService::Stub>>
        stub_cache_;
};

}  // namespace adviskv::sdm
