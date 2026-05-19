#include "sdk/sdm_route_client.h"

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>

#include "common/define.h"

namespace adviskv::sdk {

namespace {

RouteReplicaRole from_pb_role(pb::ReplicaRole role) {
    switch (role) {
        case pb::ReplicaRole::LEADER:
            return RouteReplicaRole::LEADER;
        case pb::ReplicaRole::FOLLOWER:
            return RouteReplicaRole::FOLLOWER;
        default:
            return RouteReplicaRole::UNKNOWN;
    }
}

}  // namespace

SdmRouteClient::SdmRouteClient(const KVClientConf& conf) : conf_(conf) {
    const std::string target =
        fmt::format("{}:{}", conf_.sdm_host, conf_.sdm_port);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = rpc::ShardingManagerService::NewStub(channel_);
}

Status SdmRouteClient::get_route(const Key& key, RouteInfo* route) const {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")

    rpc::GetRouteRequest request;
    request.set_db_name(conf_.db_name);
    request.set_table_name(conf_.table_name);
    request.set_key(key);

    rpc::GetRouteResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(conf_.sdm_timeout_ms));

    grpc::Status grpc_status = stub_->GetRoute(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(
            fmt::format("GetRoute RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }

    if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }

    RETURN_IF_INVALID_CONDITION(response.replicas_size() > 0,
                                "route response replicas should not empty")

    route->table_id = response.table_id();
    route->shard_id = response.shard_id();
    route->replicas.clear();
    route->replicas.reserve(response.replicas_size());
    for (const auto& replica : response.replicas()) {
        route->replicas.push_back(RouteReplica{
            .endpoint =
                Endpoint{
                    .ip = replica.endpoint().ip(),
                    .port = replica.endpoint().port(),
                },
            .role = from_pb_role(replica.role()),
        });
    }
    return Status::OK();
}

}  // namespace adviskv::sdk