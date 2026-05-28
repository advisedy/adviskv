#include "e2e_storage_test_util.h"

#include <fmt/core.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <thread>

#include "common/define.h"
#include "common/enum_convert.h"
#include "e2e_assert.h"
#include "e2e_route_util.h"
#include "storage.grpc.pb.h"

namespace adviskv::e2e {

using Clock = std::chrono::steady_clock;

bool get_replica_state_for_test(const Endpoint& endpoint, TableID table_id,
                                ShardIndex shard_id, const Options& options,
                                ReplicaState* state, std::string* error) {
    if (state == nullptr) {
        *error = "state is nullptr";
        return false;
    }

    const std::string target = endpoint.to_string();

    auto channel =
        grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = rpc::StorageService::NewStub(channel);

    rpc::TestGetReplicaStateRequest request;
    request.set_table_id(table_id);
    request.set_shard_id(shard_id);

    rpc::TestGetReplicaStateResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(options.timeout_ms));
    const grpc::Status status =
        stub->TestGetReplicaState(&client_context, request, &response);
    if (!grpc_ok(status, "TestGetReplicaState", error)) {
        return false;
    }
    if (!base_rsp_ok(response.base_rsp().code(), response.base_rsp().msg(),
                     "TestGetReplicaState", error)) {
        return false;
    }

    state->exists = response.exists();
    IGNORE_RESULT(convert_pb_to_replica_role(response.role(), state->role));
    IGNORE_RESULT(
        convert_pb_to_replica_status(response.status(), state->status));
    state->current_term = response.current_term();
    state->commit_index = response.commit_index();
    state->last_applied = response.last_applied();
    state->snapshot_index = response.snapshot_index();
    state->snapshot_term = response.snapshot_term();
    return true;
}

bool get_route_replica_states_for_test(E2EContext* context, const Key& key,
                                       const Options& options,
                                       RouteReplicaStatesForTest* states,
                                       std::string* error) {
    if (states == nullptr) {
        *error = "states is nullptr";
        return false;
    }

    states->followers.clear();
    if (!get_route(context, key, &states->route, error)) {
        return false;
    }

    bool leader_found = false;
    for (const sdk::RouteReplica& replica : states->route.replicas) {
        if (replica.role == sdk::RouteReplicaRole::LEADER) {
            states->leader = replica;
            leader_found = true;
        } else if (replica.role == sdk::RouteReplicaRole::FOLLOWER) {
            states->followers.push_back(replica);
        }
    }

    if (!leader_found) {
        *error = "leader not found";
        return false;
    }

    if (!get_replica_state_for_test(
            states->leader.endpoint, states->route.table_id,
            states->route.shard_id, options, &states->leader_state, error)) {
        return false;
    }
    return true;
}

bool wait_replica_applied_at_least_for_test(const Endpoint& endpoint,
                                            TableID table_id,
                                            ShardIndex shard_id,
                                            int64_t target_index,
                                            const Options& options,
                                            std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    std::string last_error = "not attempted";
    ReplicaState last_state;
    while (Clock::now() < deadline) {
        std::string error;
        if (get_replica_state_for_test(endpoint, table_id, shard_id, options,
                                       &last_state, &error) &&
            last_state.exists && last_state.last_applied >= target_index) {
            print_pass("wait replica apply state",
                       fmt::format("{}:{} shard={} last_applied={} target={}",
                                   endpoint.ip, endpoint.port, shard_id,
                                   last_state.last_applied, target_index));
            return true;
        }
        last_error = error.empty()
                         ? fmt::format("exists={}, last_applied={}, target={}",
                                       last_state.exists,
                                       last_state.last_applied, target_index)
                         : error;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(options.poll_interval_ms));
    }

    print_fail("wait replica apply state",
               fmt::format("{}:{} shard={} timed out: {}", endpoint.ip,
                           endpoint.port, shard_id, last_error));
    return false;
}

}  // namespace adviskv::e2e