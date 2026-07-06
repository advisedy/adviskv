#include "sdk/sdm_route_client.h"

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>

#include "common/define.h"
#include "common/proto/proto.h"
#include "sdk/config.h"
#include "sdk/log.h"
#include "sdk/model.h"

namespace adviskv::sdk {

SdmRouteClient::SdmRouteClient(const KVClientConf& conf) : conf_(conf) {
    const std::string target =
        fmt::format("{}:{}", conf_.sdm_host, conf_.sdm_port);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = sdm_rpc::SdmService::NewStub(channel_);
}

Status SdmRouteClient::get_route(const Key& key, RouteInfo* route) const {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")

    sdm_rpc::GetRouteRequest request;
    request.set_db_name(conf_.db_name);
    request.set_table_name(conf_.table_name);
    request.set_key(key);

    sdm_rpc::GetRouteResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         Milliseconds(conf_.sdm_timeout_ms));

    grpc::Status grpc_status = stub_->GetRoute(&context, request, &response);
    if (!grpc_status.ok()) {
        ADVISKV_SDK_LOG(LogLevel::ERROR,
                        "GetRoute RPC failed, sdm={}:{}, db={}, table={}, "
                        "key={}, grpc_code={}, msg={}",
                        conf_.sdm_host, conf_.sdm_port, conf_.db_name,
                        conf_.table_name, key,
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message());
        return Status::ERROR(
            fmt::format("GetRoute RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }

    if (Status status = decode_base_rsp_status(response.base_rsp());
        status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "GetRoute returns non-ok, sdm={}:{}, db={}, table={}, "
                        "key={}, code={}, msg={}",
                        conf_.sdm_host, conf_.sdm_port, conf_.db_name,
                        conf_.table_name, key, response.base_rsp().code(),
                        response.base_rsp().msg());
        return status;
    }

    RETURN_IF_INVALID_CONDITION(response.replicas_size() > 0,
                                "route response replicas should not empty")

    route->table_id = response.table_id();
    route->shard_id = response.shard_id();
    route->replicas.clear();
    route->replicas.reserve(response.replicas_size());
    for (const auto& replica : response.replicas()) {
        RouteReplica route_replica;
        RETURN_IF_INVALID_CONDITION(
            decode_pb_replica_id(replica.replica_id(), route_replica.replica_id),
            "route replica id is not valid")
        route_replica.endpoint =
            Endpoint{replica.endpoint().ip(), replica.endpoint().port()};
        ReplicaRole role = ReplicaRole::FOLLOWER;
        RETURN_IF_INVALID_CONDITION(decode_pb_raft_role(replica.role(), role),
                                    "route replica role is not valid")
        route_replica.role = role;
        route->replicas.push_back(std::move(route_replica));
    }

    {
        std::string route_res;
        for (const RouteReplica& one : route->replicas) {
            std::string one_str{fmt::format(
                "replica:[id:{}, ip:{}, port:{}, role:{}], ",
                one.replica_id.to_string(), one.endpoint.ip, one.endpoint.port,
                (int8)one.role)};
            route_res.append(std::move(one_str));
        }
        ADVISKV_SDK_LOG(LogLevel::INFO, "get route ok, route: {}", route_res);
    }

    return Status::OK();
}

}  // namespace adviskv::sdk
