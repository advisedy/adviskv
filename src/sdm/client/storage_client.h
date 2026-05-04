#pragma once

#include <grpcpp/channel.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
#include "storage.grpc.pb.h"

namespace adviskv::sdm {

class StorageClient {
   public:
    StorageClient() = default;

    Status create_replica(const CreateReplicaParam& param);

   private:
    rpc::StorageService::Stub* make_stub(const std::string& ip, int32_t port);

    // stub_cache_ 的key我们是用IP和port结合在一起
    std::unordered_map<std::string, std::unique_ptr<rpc::StorageService::Stub>>
        stub_cache_;
};

}  // namespace adviskv::sdm
