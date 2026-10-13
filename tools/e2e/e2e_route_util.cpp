#include "e2e_route_util.h"

#include <chrono>
#include <utility>

#include <fmt/core.h>
#include <grpcpp/client_context.h>

#include "common/proto/codec.h"
#include "common/shard_range.h"
#include "e2e_assert.h"
#include "sdk/model.h"

namespace adviskv::e2e {

bool get_route(E2EContext* context, const Key& key, ShardRoute* route, std::string* error) {
    if (route == nullptr) {
        *error = "route is nullptr";
        return false;
    }

    const Options& options = context->options();
    meta_rpc::GetTableRoutesRequest request;
    request.set_db_name(options.db);
    request.set_table_name(options.table);

    meta_rpc::GetTableRoutesResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(3000));
    const grpc::Status status = context->meta()->GetTableRoutes(&client_context, request, &response);
    if (!grpc_ok(status, "GetTableRoutes", error)) {
        return false;
    }
    if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
        *error = fmt::format("GetTableRoutes failed, code={}, msg={}", response.base_rsp().code(),
                             response.base_rsp().msg());
        return false;
    }

    sdk::TableRouteInfo table;
    table.table_id = response.table_id();
    table.shard_count = response.shard_count();
    table.routes.reserve(static_cast<size_t>(response.routes_size()));
    for (const auto& route_pb : response.routes()) {
        ShardRoute one;
        one.shard_id.table_id = table.table_id;
        if (!codec::decode(route_pb, one)) {
            *error = "table route is not valid";
            return false;
        }
        table.routes.push_back(std::move(one));
    }

    const size_t idx = sdk::locate_route(table, key);
    if (idx >= table.routes.size()) {
        *error = "route not found for key";
        return false;
    }
    *route = table.routes[idx];
    return true;
}

bool wait_route_has_leader(E2EContext* context, const Key& key, RouteReplica* leader) {
    const Options& options = context->options();
    std::string last_error;
    return eventually(
            "route has leader", options,
            [&]() {
                ShardRoute route;
                std::string error;
                if (!get_route(context, key, &route, &error)) {
                    return CheckResult::fail(error);
                }
                for (const RouteReplica& replica : route.replicas) {
                    if (replica.role == ReplicaRole::LEADER) {
                        if (leader != nullptr) {
                            *leader = replica;
                        }
                        return CheckResult::pass(fmt::format("{}:{} shard={}", replica.endpoint.ip,
                                                             replica.endpoint.port, route.shard_id.shard_index));
                    }
                }
                return CheckResult::fail("leader replica not found");
            },
            &last_error);
}

bool print_current_leader(E2EContext* context, const std::string& key) {
    RouteReplica leader;
    if (!wait_route_has_leader(context, key, &leader)) {
        return false;
    }
    fmt::print("[ ROUTE_LEADER ] {}:{}\n", leader.endpoint.ip, leader.endpoint.port);
    return true;
}

}  // namespace adviskv::e2e
