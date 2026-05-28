#include "e2e_route_util.h"

#include <fmt/core.h>

#include <chrono>
#include <utility>

#include "e2e_assert.h"

namespace adviskv::e2e {

namespace {

// TODO 这个回头可以放到convert文件里面，不在这放着
sdk::RouteReplicaRole from_pb_role(pb::ReplicaRole role) {
    switch (role) {
        case pb::ReplicaRole::LEADER:
            return sdk::RouteReplicaRole::LEADER;
        case pb::ReplicaRole::FOLLOWER:
            return sdk::RouteReplicaRole::FOLLOWER;
        default:
            return sdk::RouteReplicaRole::UNKNOWN;
    }
}

}  // namespace

bool get_route(E2EContext* context, const Key& key, sdk::RouteInfo* route,
               std::string* error) {
    if (route == nullptr) {
        *error = "route is nullptr";
        return false;
    }

    const Options& options = context->options();
    rpc::GetRouteRequest request;
    request.set_db_name(options.db);
    request.set_table_name(options.table);
    request.set_key(key);

    rpc::GetRouteResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(3000));
    const grpc::Status status =
        context->sdm()->GetRoute(&client_context, request, &response);
    if (!grpc_ok(status, "GetRoute", error)) {
        return false;
    }
    if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
        *error =
            fmt::format("GetRoute failed, code={}, msg={}",
                        response.base_rsp().code(), response.base_rsp().msg());
        return false;
    }

    route->table_id = response.table_id();
    route->shard_id = response.shard_id();
    route->replicas.clear();
    route->replicas.reserve(response.replicas_size());
    for (const auto& replica : response.replicas()) {
        sdk::RouteReplica out;
        out.endpoint =
            Endpoint{replica.endpoint().ip(), replica.endpoint().port()};
        out.role = from_pb_role(replica.role());
        route->replicas.push_back(std::move(out));
    }
    return true;
}

bool wait_route_has_leader(E2EContext* context, const Key& key,
                           sdk::RouteReplica* leader) {
    const Options& options = context->options();
    std::string last_error;
    return eventually(
        "route has leader", options,
        [&]() {
            sdk::RouteInfo route;
            std::string error;
            if (!get_route(context, key, &route, &error)) {
                return CheckResult::fail(error);
            }
            for (const sdk::RouteReplica& replica : route.replicas) {
                if (replica.role == sdk::RouteReplicaRole::LEADER) {
                    if (leader != nullptr) {
                        *leader = replica;
                    }
                    return CheckResult::pass(
                        fmt::format("{}:{} shard={}", replica.endpoint.ip,
                                    replica.endpoint.port, route.shard_id));
                }
            }
            return CheckResult::fail("leader replica not found");
        },
        &last_error);
}

bool print_current_leader(E2EContext* context, const std::string& key) {
    sdk::RouteReplica leader;
    if (!wait_route_has_leader(context, key, &leader)) {
        return false;
    }
    fmt::print("[ ROUTE_LEADER ] {}:{}\n", leader.endpoint.ip,
               leader.endpoint.port);
    return true;
}

}  // namespace adviskv::e2e