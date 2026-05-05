#pragma once

#include <memory>
#include <string>

#include <grpcpp/channel.h>

#include "common/status.h"
#include "common/type.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "sdm.grpc.pb.h"

namespace adviskv::sdk {

class SdmRouteClient {
   public:
    explicit SdmRouteClient(const KVClientConf& conf);

    Status get_route(const std::string& db_name, const std::string& table_name,
                     const Key& key, RouteInfo* route) const;

   private:
    KVClientConf conf_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<rpc::ShardingManagerService::Stub> stub_;
};

}  // namespace adviskv::sdk