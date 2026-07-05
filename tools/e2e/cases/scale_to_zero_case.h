#pragma once

#include <fmt/core.h>

#include <string>

#include "common/status.h"
#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
#include "e2e_route_util.h"
#include "e2e_table_util.h"
#include "sdk/client.h"

namespace adviskv::e2e {

inline bool run_scale_to_zero_case(const Options& options) {
    constexpr int32_t kExpandedReplicaCount = 1;
    constexpr const char* kPrefix = "scale-to-zero";

    auto expect_status_code = [](const std::string& name, const Status& status,
                                 StatusCode expected) {
        if (status.code() != expected) {
            print_fail(name,
                       fmt::format("status={}, expected_code={}",
                                   status.to_string(), to_rpc_code(expected)));
            return false;
        }
        print_pass(name, status.to_string());
        return true;
    };

    if (!validate_key_count(options)) {
        return false;
    }
    if (options.replica_count != 0) {
        print_fail("validate options", "scale_to_zero requires --replica_count=0");
        return false;
    }

    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }
    if (!wait_table_replica_count(&context, 0)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    const Key key = std::string(kPrefix) + "-key-000";
    const Value value = std::string(kPrefix) + "-value-000";

    if (!expect_status_code("sdk put zero replica", client.put(key, value),
                            StatusCode::ROUTE_NOT_FOUND)) {
        return false;
    }

    Value out;
    if (!expect_status_code("sdk get zero replica", client.get(key, &out),
                            StatusCode::ROUTE_NOT_FOUND)) {
        return false;
    }

    if (!expect_status_code("sdk delete zero replica", client.del(key),
                            StatusCode::ROUTE_NOT_FOUND)) {
        return false;
    }

    std::string error;
    if (!alter_table_replica_count(&context, kExpandedReplicaCount,
                                   &error)) {
        print_fail("alter table replica_count", error);
        return false;
    }
    print_pass("alter table replica_count",
               fmt::format("target={}", kExpandedReplicaCount));

    if (!wait_table_replica_count(&context, kExpandedReplicaCount)) {
        return false;
    }
    if (!wait_route_has_leader(&context, key, nullptr)) {
        return false;
    }

    return write_dataset(&client, options, kPrefix) &&
           verify_dataset(&client, options, kPrefix);
}

}  // namespace adviskv::e2e
