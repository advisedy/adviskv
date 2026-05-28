#pragma once

#include <fmt/format.h>

#include <chrono>

#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_options.h"
#include "e2e_storage_test_util.h"
#include "heavy_case_util.h"
#include "sdk/model.h"

// TODO 感觉这里的实现有点啰嗦了，以后要好好整改一下这些API

namespace adviskv::e2e {
namespace {
constexpr const char* kFollowerSnapshotCatchupBasePrefix =
    "follower-snapshot-catchup-base";
constexpr const char* kFollowerSnapshotCatchupGapPrefix =
    "follower-snapshot-catchup-gap";
constexpr const char* kFollowerSnapshotCatchupAfterKey =
    "follower-snapshot-catchup-after";
constexpr const char* kFollowerSnapshotCatchupAfterValue =
    "follower-snapshot-catchup-after-value";
constexpr int32_t kFollowerSnapshotCatchupBaseCount = 16;
constexpr int32_t kFollowerSnapshotCatchupGapCount = 1050;
}  // namespace

/*
整个流程是：
写key，follower崩溃，然后再写一些key（触发快照），然后follower恢复，
然后写一个key，然后记录leader的last_apply，然后比较follower的，要大于等于leader的last_apply
*/
inline bool run_follower_snapshot_catchup_prepare_case(const Options& options) {
    // 创建db + 创建table + 写入 key +
    // （记得输出route，这样py那边方便判断follower）， 然后就该崩溃了。
    Options base_options =
        heavy::with_key_count(options, kFollowerSnapshotCatchupBaseCount);
    if (base_options.key_count <= 0) {
        print_fail("validate options", "--key_count must be positive");
        return false;
    }

    if (!heavy::prepare_and_print_route(
            base_options,
            heavy::first_dataset_key(kFollowerSnapshotCatchupBasePrefix))) {
        return false;
    }
    print_pass("prepare and print route", "ok");

    sdk::KVClient client = make_kv_client(base_options);
    return write_dataset(&client, base_options,
                         kFollowerSnapshotCatchupBasePrefix);
}

inline bool run_follower_snapshot_catchup_write_gap_case(
    const Options& options) {
    Options gap_options =
        heavy::with_key_count(options, kFollowerSnapshotCatchupGapCount);

    if (!heavy::wait_existing_table(
            gap_options,
            heavy::first_dataset_key(kFollowerSnapshotCatchupGapPrefix))) {
        return false;
    }
    sdk::KVClient client = make_kv_client(gap_options);
    if (!write_dataset(&client, gap_options,
                       kFollowerSnapshotCatchupGapPrefix)) {
        return false;
    }
    E2EContext context{gap_options};
    std::string last_error;
    RouteReplicaStatesForTest states;

    if (!get_route_replica_states_for_test(
            &context,
            heavy::first_dataset_key(kFollowerSnapshotCatchupGapPrefix),
            &states, &last_error)) {
        print_fail("get route replica states", last_error);
        return false;
    }

    if (!wait_replica_snapshot_index_at_least_for_test(
            states.leader.endpoint, states.route.table_id,
            states.route.shard_id, 1, gap_options,
            std::chrono::milliseconds(gap_options.timeout_ms))) {
        return false;
    }

    print_pass("leader snapshot", "ok");
    return true;
}

inline bool run_follower_snapshot_catchup_verify_case(const Options& options) {
    // 流程： 写入key +
    // 然后记录leader的last_apply，然后比较follower的，要大于等于leader的last_apply

    Options base_options =
        heavy::with_key_count(options, kFollowerSnapshotCatchupBaseCount);
    Options gap_options =
        heavy::with_key_count(options, kFollowerSnapshotCatchupGapCount);

    if (!heavy::wait_existing_table(
            options,
            heavy::first_dataset_key(kFollowerSnapshotCatchupBasePrefix))) {
        return false;
    }
    sdk::KVClient client = make_kv_client(options);

    if (!verify_dataset(&client, base_options,
                        kFollowerSnapshotCatchupBasePrefix)) {
        return false;
    }
    if (!verify_dataset(&client, gap_options,
                        kFollowerSnapshotCatchupGapPrefix)) {
        return false;
    }
    print_pass("verify before's dataset", "ok");
    if (!wait_status("sdk put follower-snapshot-catchup-after", options, [&]() {
            return client.put(kFollowerSnapshotCatchupAfterKey,
                              kFollowerSnapshotCatchupAfterValue);
        })) {
        return false;
    }
    if (!wait_get_value(&client, options, kFollowerSnapshotCatchupAfterKey,
                        kFollowerSnapshotCatchupAfterValue)) {
        return false;
    }
    print_pass("put new key", "ok");

    E2EContext context{options};
    std::string last_error;
    RouteReplicaStatesForTest replica_states;
    if (!get_route_replica_states_for_test(&context,
                                           kFollowerSnapshotCatchupAfterKey,
                                           &replica_states, &last_error)) {
        print_fail("get route replica states", last_error);
        return false;
    }
    if (!wait_replica_snapshot_index_at_least_for_test(
            replica_states.leader.endpoint, replica_states.route.table_id,
            replica_states.route.shard_id, 1, options,
            std::chrono::milliseconds(options.timeout_ms))) {
        return false;
    }
    if (!get_route_replica_states_for_test(&context,
                                           kFollowerSnapshotCatchupAfterKey,
                                           &replica_states, &last_error)) {
        print_fail("get route replica states", last_error);
        return false;
    }

    int64 target_index = replica_states.leader_state.last_applied;
    print_pass("get leader last_applied",
               fmt::format("ok, last_applied:{}", target_index));
    for (const auto& replica : replica_states.followers) {
        if (!wait_replica_apply_index_at_least_for_test(
                replica.endpoint, replica_states.route.table_id,
                replica_states.route.shard_id, target_index, options,
                std::chrono::milliseconds(options.timeout_ms))) {
            return false;
        }
        if (!wait_replica_snapshot_index_at_least_for_test(
                replica.endpoint, replica_states.route.table_id,
                replica_states.route.shard_id, 1, options,
                std::chrono::milliseconds(options.timeout_ms))) {
            return false;
        }
    }

    print_pass("run_follower_snapshot_catchup_verify_case", "ok");

    return true;
}

}  // namespace adviskv::e2e