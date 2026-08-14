#include "sdk/topo_route_client.h"

#include <chrono>
#include <string>
#include <unordered_set>
#include <vector>

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common/define.h"
#include "common/proto/proto.h"
#include "sdk/config.h"
#include "sdk/log.h"
#include "sdk/model.h"

namespace adviskv::sdk {

TopoRouteClient::TopoRouteClient(const KVClientConf& conf) : conf_(conf) {
    const std::string target = fmt::format("{}:{}", conf_.topo_host, conf_.topo_port);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = topo_rpc::TopoService::NewStub(channel_);
}

Status TopoRouteClient::get_table_routes(TableRouteInfo* table_routes, Milliseconds timeout) const {
    RETURN_IF_NULLPTR(table_routes, "table_routes should not be nullptr")

    topo_rpc::GetTableRoutesRequest request;
    request.set_db_name(conf_.db_name);
    request.set_table_name(conf_.table_name);

    topo_rpc::GetTableRoutesResponse response;
    grpc::ClientContext context;
    Milliseconds real_timeout = timeout > Milliseconds::zero() ? timeout : Milliseconds(conf_.rpc_timeout_ms);
    context.set_deadline(std::chrono::system_clock::now() + real_timeout);

    grpc::Status grpc_status = stub_->GetTableRoutes(&context, request, &response);
    if (!grpc_status.ok()) {
        ADVISKV_SDK_LOG(LogLevel::ERROR,
                        "GetTableRoutes RPC failed, topo={}:{}, db={}, table={}, "
                        "grpc_code={}, msg={}",
                        conf_.topo_host, conf_.topo_port, conf_.db_name, conf_.table_name,
                        static_cast<int>(grpc_status.error_code()), grpc_status.error_message());
        return Status::ERROR(fmt::format("GetTableRoutes RPC failed, grpc code = {}, msg = {}",
                                         static_cast<int>(grpc_status.error_code()), grpc_status.error_message()));
    }

    if (Status status = decode_base_rsp_status(response.base_rsp()); status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "GetTableRoutes returns non-ok, topo={}:{}, db={}, table={}, "
                        "code={}, msg={}",
                        conf_.topo_host, conf_.topo_port, conf_.db_name, conf_.table_name, response.base_rsp().code(),
                        response.base_rsp().msg());
        return status;
    }

    RETURN_IF_INVALID_CONDITION(response.table_id() >= 0, "table routes table_id is invalid")
    RETURN_IF_INVALID_CONDITION(response.shard_count() > 0, "table routes shard_count is invalid")

    int32 shard_count = response.shard_count();
    TableRouteInfo next;
    next.table_id = response.table_id();
    next.shard_count = shard_count;
    next.routes.resize(to<size_t>(shard_count));

    int32 decoded_count{0};
    for (const auto& route_pb : response.routes()) {
        ShardIndex shard_id = route_pb.shard_id();
        RETURN_IF_INVALID_CONDITION(
                shard_id >= 0 && shard_id < shard_count,
                fmt::format("table route shard_id invalid, shard_id={}, shard_count={}", shard_id, shard_count))
        RETURN_IF_INVALID_CONDITION(route_pb.replicas_size() > 0,
                                    fmt::format("table route replicas should not empty, shard_id={}", shard_id))

        RouteInfo route;
        route.table_id = next.table_id;
        route.shard_id = shard_id;
        route.replicas.reserve(route_pb.replicas_size());
        for (const auto& replica_pb : route_pb.replicas()) {
            RouteReplica route_replica;
            RETURN_IF_INVALID_CONDITION(decode_pb_replica_id(replica_pb.replica_id(), route_replica.replica_id),
                                        "route replica id is not valid")
            route_replica.endpoint = Endpoint{replica_pb.endpoint().ip(), replica_pb.endpoint().port()};
            ReplicaRole role = ReplicaRole::FOLLOWER;
            RETURN_IF_INVALID_CONDITION(decode_pb_raft_role(replica_pb.role(), role), "route replica role is not valid")
            route_replica.role = role;
            route.replicas.push_back(std::move(route_replica));
        }

        next.routes[to<size_t>(shard_id)] = std::move(route);
        ++decoded_count;
    }

    RETURN_IF_INVALID_CONDITION(
            decoded_count == shard_count,
            fmt::format("table routes count mismatch, routes={}, shard_count={}", decoded_count, shard_count))

    *table_routes = std::move(next);
    return Status::OK();
}

}  // namespace adviskv::sdk