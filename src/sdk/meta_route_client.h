#pragma once

#include <memory>

#include <grpcpp/channel.h>

#include "common/proto/proto.h"
#include "common/status.h"
#include "meta.grpc.pb.h"
#include "sdk/config.h"
#include "sdk/model.h"

namespace adviskv::sdk {

class MetaRouteClient {
public:
    explicit MetaRouteClient(const KVClientConf& conf);

    Status get_table_routes(TableRouteInfo* table_routes, Milliseconds timeout = Milliseconds::zero(),
                            bool include_unavailable = false) const;

private:
    KVClientConf conf_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<meta_rpc::MetaService::Stub> stub_;
};

}  // namespace adviskv::sdk
