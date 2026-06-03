#pragma once

#include <fmt/core.h>

#include <string>

#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
#include "e2e_table_util.h"

namespace adviskv::e2e {
namespace {

inline bool run_create_table_crash_prepare_case(const Options& options,
                                                const std::string& name) {
    E2EContext context(options);
    std::string error;
    if (!create_db(&context, &error)) {
        print_fail("create database", error);
        return false;
    }
    print_pass("create database", options.db);

    if (create_table(&context, &error)) {
        print_pass("create table request",
                   fmt::format("{} accepted before crash window", name));
        return true;
    }

    print_pass("create table request",
               fmt::format("{} observed expected interruption: {}", name,
                           error));
    return true;
}

inline bool run_create_table_crash_verify_case(const Options& options,
                                               const std::string& prefix,
                                               const std::string& after_suffix) {
    E2EContext context(options);
    if (!wait_table_normal(&context)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    const std::string key = prefix + "-" + after_suffix;
    const std::string value = prefix + "-value-" + after_suffix;
    if (!wait_status(fmt::format("sdk put {}", key), options,
                     [&]() { return client.put(key, value); })) {
        return false;
    }
    return wait_get_value(&client, options, key, value);
}

}  // namespace

inline bool run_create_table_crash_before_persist_prepare_case(
    const Options& options) {
    return run_create_table_crash_prepare_case(options,
                                               "before-persist-crash");
}

inline bool run_create_table_crash_before_persist_verify_case(
    const Options& options) {
    return run_create_table_crash_verify_case(options,
                                              "create-table-before-persist",
                                              "after-recovery");
}

inline bool run_create_table_crash_after_persist_prepare_case(
    const Options& options) {
    return run_create_table_crash_prepare_case(options,
                                               "after-persist-crash");
}

inline bool run_create_table_crash_after_persist_verify_case(
    const Options& options) {
    return run_create_table_crash_verify_case(options,
                                              "create-table-after-persist",
                                              "after-recovery");
}

}  // namespace adviskv::e2e