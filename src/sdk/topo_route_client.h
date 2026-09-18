#pragma once

#include <memory>

#include <grpcpp/channel.h>

#include "common/proto/proto.h"
#include "common/status.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "topo.grpc.pb.h"

namespace adviskv::sdk {

class TopoRouteClient {
public:
    explicit TopoRouteClient(const KVClientConf& conf);

    Status get_table_routes(TableRouteInfo* table_routes,
                            Milliseconds timeout = Milliseconds::zero(), bool include_unavailable = false) const;

private:
    KVClientConf conf_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<topo_rpc::TopoService::Stub> stub_;
};

}  // namespace adviskv::sdk
