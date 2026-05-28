#pragma once

#include <chrono>
#include <string>

#include "common/type.h"
#include "e2e_options.h"

namespace adviskv::e2e {

struct ReplicaApplyState {
    bool exists = false;
    ReplicaRole role = ReplicaRole::FOLLOWER;
    ReplicaStatus status = ReplicaStatus::ADDING;
    Term current_term = 0;
    int64 commit_index = 0;
    int64_t last_applied = 0;
};

bool get_replica_apply_state_for_test(const Endpoint& endpoint,
                                      TableID table_id, ShardIndex shard_id,
                                      const Options& options,
                                      ReplicaApplyState* state,
                                      std::string* error);

bool wait_replica_applied_at_least_for_test(const Endpoint& endpoint,
                                            TableID table_id,
                                            ShardIndex shard_id,
                                            int64_t target_index,
                                            const Options& options,
                                            std::chrono::milliseconds timeout);

}  // namespace adviskv::e2e