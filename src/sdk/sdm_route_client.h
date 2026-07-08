#pragma once

#include <memory>

#include <grpcpp/channel.h>

#include "common/proto/proto.h"
#include "common/status.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "sdm.grpc.pb.h"

namespace adviskv::sdk {

class SdmRouteClient {
public:
    explicit SdmRouteClient(const KVClientConf& conf);

    Status get_table_routes(TableRouteInfo* table_routes) const;

private:
    KVClientConf conf_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<sdm_rpc::SdmService::Stub> stub_;
};

}  // namespace adviskv::sdk