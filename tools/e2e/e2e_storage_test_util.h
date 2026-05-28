#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "common/type.h"
#include "e2e_options.h"
#include "sdk/model.h"

namespace adviskv::e2e {

class E2EContext;

struct ReplicaState {
    bool exists = false;
    ReplicaRole role = ReplicaRole::FOLLOWER;
    ReplicaStatus status = ReplicaStatus::ADDING;
    Term current_term = 0;
    int64 commit_index = 0;
    int64_t last_applied = 0;
    int64_t snapshot_index = 0;
    Term snapshot_term = 0;
};

bool get_replica_state_for_test(const Endpoint& endpoint, TableID table_id,
                                ShardIndex shard_id, const Options& options,
                                ReplicaState* state, std::string* error);

struct RouteReplicaStatesForTest {
    sdk::RouteInfo route;
    sdk::RouteReplica leader;
    ReplicaState leader_state;
    std::vector<sdk::RouteReplica> followers;
};

bool get_route_replica_states_for_test(E2EContext* context, const Key& key,
                                       const Options& options,
                                       RouteReplicaStatesForTest* states,
                                       std::string* error);

bool wait_replica_applied_at_least_for_test(const Endpoint& endpoint,
                                            TableID table_id,
                                            ShardIndex shard_id,
                                            int64_t target_index,
                                            const Options& options,
                                            std::chrono::milliseconds timeout);

}  // namespace adviskv::e2e