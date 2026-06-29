#pragma once

#include <grpcpp/channel.h>

#include <memory>
#include <string>

#include "common/proto/rpc_alias.h"
#include "common/status.h"
#include "common/type.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "sdm.grpc.pb.h"

namespace adviskv::sdk {

class SdmRouteClient {
   public:
    explicit SdmRouteClient(const KVClientConf& conf);

    Status get_route(const Key& key, RouteInfo* route) const;

   private:
    KVClientConf conf_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<sdm_rpc::SdmService::Stub> stub_;
};

}  // namespace adviskv::sdk