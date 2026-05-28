#pragma once

#include <fmt/format.h>

#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_options.h"
#include "e2e_route_util.h"
#include "e2e_storage_test_util.h"
#include "heavy_case_util.h"
#include "sdk/model.h"

namespace adviskv::e2e {
namespace {
constexpr const char* kFollowerLogCatchupBasePrefix =
    "follower-log-catchup-base";
constexpr const char* kFollowerLogCatchupGapPrefix =
    "follower-log-catchup-gap";
constexpr const char* kFollowerLogCatchupAfterKey =
    "follower-log-catchup-after";
constexpr const char* kFollowerLogCatchupAfterValue =
    "follower-log-catchup-after-value";
}  // namespace

/*
整个流程是：
写key，follower崩溃，然后再写一些key（不要太多，不触发快照），然后follower恢复，
然后写一个key，然后记录leader的last_apply，然后比较follower的，要大于等于leader的last_apply
*/
inline bool run_follower_log_catchup_prepare_case(const Options& options) {
    // 创建db + 创建table + 写入 key +
    // （记得输出route，这样py那边方便判断follower）， 然后就该崩溃了。
    if (options.key_count <= 0) {
        print_fail("validate options", "--key_count must be positive");
        return false;
    }

    if (!heavy::prepare_and_print_route(
            options, heavy::first_dataset_key(kFollowerLogCatchupBasePrefix))) {
        return false;
    }
    print_pass("prepare and print route", "ok");

    sdk::KVClient client = make_kv_client(options);
    return write_dataset(&client, options, kFollowerLogCatchupBasePrefix);
}

inline bool run_follower_log_catchup_write_gap_case(const Options& options) {
    if (options.key_count > 100) {
        print_fail("option key count", "is too big. please set small");
        return false;
    }
    return heavy::write_gap_case(options, kFollowerLogCatchupGapPrefix,
                                 options.key_count);
}

inline bool run_follower_log_catchup_verify_case(const Options& options) {
    // 流程： 写入key +
    // 然后记录leader的last_apply，然后比较follower的，要大于等于leader的last_apply

    if (!heavy::wait_existing_table(
            options, heavy::first_dataset_key(kFollowerLogCatchupBasePrefix))) {
        return false;
    }
    sdk::KVClient client = make_kv_client(options);
    if (!verify_dataset(&client, options, kFollowerLogCatchupBasePrefix)) {
        return false;
    }
    if (!verify_dataset(&client, options, kFollowerLogCatchupGapPrefix)) {
        return false;
    }
    print_pass("verify before's dataset", "ok");
    if (!wait_status("sdk put follower-log-catchup-after", options, [&]() {
            return client.put(kFollowerLogCatchupAfterKey,
                              kFollowerLogCatchupAfterValue);
        })) {
        return false;
    }
    if (!wait_get_value(&client, options, kFollowerLogCatchupAfterKey,
                        kFollowerLogCatchupAfterValue)) {
        return false;
    }
    print_pass("put new key", "ok");
    
 // if(!wait_replica_applied_at_least_for_test(const Endpoint &endpoint,
    // TableID table_id, ShardIndex shard_id, int64_t target_index, const
    // Options &options, std::chrono::milliseconds timeout))
    E2EContext context{options};
    sdk::RouteInfo route;
    std::string last_error;
    if (!get_route(&context, kFollowerLogCatchupAfterKey, &route,
                   &last_error)) {
        return false;
    }

    sdk::RouteReplica* leader = nullptr;
    for (sdk::RouteReplica& replica : route.replicas) {
        if (replica.role == sdk::RouteReplicaRole::LEADER) {
            leader = &replica;
            break;
        }
    }

    if (leader == nullptr) {
        print_fail("find leader", "leader not found");
        return false;
    }

    ReplicaState leader_state;
    if (!get_replica_state_for_test(leader->endpoint, route.table_id,
                                    route.shard_id, options, &leader_state,
                                    &last_error)) {
        print_fail("get leader replica state", last_error);
        return false;
    }

    int64 target_index = leader_state.last_applied;
    print_pass("get leader last_applied",
               fmt::format("ok, last_applied:{}", target_index));
    for (const auto& replica : route.replicas) {
        if (replica.role == sdk::RouteReplicaRole::LEADER) {
            continue;
        }

        if (!wait_replica_applied_at_least_for_test(
                replica.endpoint, route.table_id, route.shard_id, target_index,
                options, std::chrono::seconds(30))) {
            return false;
        }
    }

    print_pass("run_follower_log_catchup_verify_case", "ok");

    return true;
}

}  // namespace adviskv::e2e
