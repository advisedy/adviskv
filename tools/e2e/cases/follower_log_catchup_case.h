#pragma once

#include <fmt/format.h>

#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_options.h"
#include "e2e_storage_test_util.h"
#include "heavy_case_util.h"
#include "sdk/model.h"

namespace adviskv::e2e {
namespace {
constexpr const char* K_FOLLOWER_LOG_CATCHUP_BASE_PREFIX =
    "follower-log-catchup-base";
constexpr const char* K_FOLLOWER_LOG_CATCHUP_GAP_PREFIX = "follower-log-catchup-gap";
constexpr const char* K_FOLLOWER_LOG_CATCHUP_AFTER_KEY =
    "follower-log-catchup-after";
constexpr const char* K_FOLLOWER_LOG_CATCHUP_AFTER_VALUE =
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
            options, heavy::first_dataset_key(K_FOLLOWER_LOG_CATCHUP_BASE_PREFIX))) {
        return false;
    }
    print_pass("prepare and print route", "ok");

    sdk::KVClient client = make_kv_client(options);
    return write_dataset(&client, options, K_FOLLOWER_LOG_CATCHUP_BASE_PREFIX);
}

inline bool run_follower_log_catchup_write_gap_case(const Options& options) {
    if (options.key_count > 100) {
        print_fail("option key count", "is too big. please set small");
        return false;
    }
    if (!heavy::wait_existing_table(
            options, heavy::first_dataset_key(K_FOLLOWER_LOG_CATCHUP_GAP_PREFIX))) {
        return false;
    }
    sdk::KVClient client = make_kv_client(options);
    return write_dataset(&client, options, K_FOLLOWER_LOG_CATCHUP_GAP_PREFIX);
}

inline bool run_follower_log_catchup_verify_case(const Options& options) {
    // 流程： 写入key +
    // 然后记录leader的last_apply，然后比较follower的，要大于等于leader的last_apply

    if (!heavy::wait_existing_table(
            options, heavy::first_dataset_key(K_FOLLOWER_LOG_CATCHUP_BASE_PREFIX))) {
        return false;
    }
    sdk::KVClient client = make_kv_client(options);
    if (!verify_dataset(&client, options, K_FOLLOWER_LOG_CATCHUP_BASE_PREFIX)) {
        return false;
    }
    if (!verify_dataset(&client, options, K_FOLLOWER_LOG_CATCHUP_GAP_PREFIX)) {
        return false;
    }
    print_pass("verify before's dataset", "ok");
    if (!wait_status("sdk put follower-log-catchup-after", options, [&]() {
            return client.put(K_FOLLOWER_LOG_CATCHUP_AFTER_KEY,
                              K_FOLLOWER_LOG_CATCHUP_AFTER_VALUE);
        })) {
        return false;
    }
    if (!wait_get_value(&client, options, K_FOLLOWER_LOG_CATCHUP_AFTER_KEY,
                        K_FOLLOWER_LOG_CATCHUP_AFTER_VALUE)) {
        return false;
    }
    print_pass("put new key", "ok");

    E2EContext context{options};
    std::string last_error;
    RouteReplicaStatesForTest replica_states;
    if (!get_route_replica_states_for_test(&context,
                                           K_FOLLOWER_LOG_CATCHUP_AFTER_KEY,
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
                std::chrono::seconds(30))) {
            return false;
        }
    }

    print_pass("run_follower_log_catchup_verify_case", "ok");

    return true;
}

}  // namespace adviskv::e2e