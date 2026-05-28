#pragma once

#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <string>

#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
#include "e2e_route_util.h"
#include "e2e_table_util.h"
#include "sdk/client.h"
#include "sdk/model.h"

namespace adviskv::e2e::heavy {

constexpr int32_t kSnapshotTriggerCount = 1050;
constexpr int32_t kWalTailCount = 16;
constexpr auto kSnapshotWait = std::chrono::seconds(6);

inline int32_t heavy_count(const Options& options) {
    return std::max(options.key_count, kSnapshotTriggerCount);
}

inline Options with_key_count(const Options& options, int32_t key_count) {
    Options counted = options;
    counted.key_count = key_count;
    return counted;
}

inline std::string first_dataset_key(const std::string& prefix) {
    return make_case_kvs(prefix, 1).front().first;
}

inline bool print_route_replicas(E2EContext* context, const std::string& key) {
    sdk::RouteInfo route;
    std::string error;
    if (!get_route(context, key, &route, &error)) {
        print_fail("get route replicas", error);
        return false;
    }

    for (const sdk::RouteReplica& replica : route.replicas) {
        const char* role = "UNKNOWN";
        if (replica.role == sdk::RouteReplicaRole::LEADER) {
            role = "LEADER";
        } else if (replica.role == sdk::RouteReplicaRole::FOLLOWER) {
            role = "FOLLOWER";
        }
        fmt::print("[ ROUTE_REPLICA ] {}:{} role={}\n", replica.endpoint.ip,
                   replica.endpoint.port, role);
        if (replica.role == sdk::RouteReplicaRole::LEADER) {
            fmt::print("[ ROUTE_LEADER ] {}:{}\n", replica.endpoint.ip,
                       replica.endpoint.port);
        }
    }
    return true;
}

inline bool prepare_and_print_route(const Options& options,
                                    const std::string& probe_key) {
    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }
    if (!wait_route_has_leader(&context, probe_key, nullptr)) {
        return false;
    }
    return print_route_replicas(&context, probe_key);
}

inline bool wait_existing_table(const Options& options,
                                const std::string& probe_key) {
    E2EContext context(options);
    if (!wait_table_normal(&context)) {
        return false;
    }
    return wait_route_has_leader(&context, probe_key, nullptr);
}

}  // namespace adviskv::e2e::heavy