#include "cases/restart_persistence_case.h"

#include <fmt/core.h>

#include <string>
#include <vector>

#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_kv_util.h"
#include "sdk/client.h"

namespace adviskv::e2e {
namespace {

std::vector<std::string> make_keys(const Options& options) {
    std::vector<std::string> keys;
    keys.reserve(options.key_count);
    for (int32_t i = 0; i < options.key_count; ++i) {
        keys.push_back(fmt::format("restart-key-{:03d}", i));
    }
    return keys;
}

std::string value_for_index(int32_t index) {
    return fmt::format("restart-value-{:03d}", index);
}

bool seed_data(sdk::KVClient* client, const Options& options) {
    const std::vector<std::string> keys = make_keys(options);
    for (int32_t i = 0; i < options.key_count; ++i) {
        const std::string value = value_for_index(i);
        if (!wait_status(fmt::format("sdk seed put {}", keys[i]), options,
                         [&]() { return client->put(keys[i], value); })) {
            return false;
        }
    }

    if (!wait_status("sdk seed overwrite first key", options, [&]() {
            return client->put(keys.front(), "restart-value-overwritten");
        })) {
        return false;
    }

    return wait_status("sdk seed delete last key", options,
                       [&]() { return client->del(keys.back()); });
}

bool verify_seeded_data(sdk::KVClient* client, const Options& options) {
    const std::vector<std::string> keys = make_keys(options);
    if (!wait_get_value(client, options, keys.front(),
                        "restart-value-overwritten")) {
        return false;
    }

    for (int32 i = 1; i + 1 < options.key_count; ++i) {
        if (!wait_get_value(client, options, keys[i], value_for_index(i))) {
            return false;
        }
    }

    return wait_key_not_found(client, options, keys.back());
}

bool validate_options(const Options& options) {
    if (options.key_count < 2) {
        print_fail("validate options", "--key_count must be >= 2");
        return false;
    }
    return true;
}

}  // namespace

bool run_restart_persistence_seed_case(const Options& options) {
    fmt::print("{} {}\n",
               colorize(COLOR::BOLD, "AdvisKV E2E Restart Persistence Seed"),
               colorize(COLOR::BLUE, "(write data before full restart)"));

    if (!validate_options(options)) {
        return false;
    }

    E2EContext context(options);
    std::string error;
    if (!create_db(&context, &error)) {
        print_fail("create database", error);
        return false;
    }
    print_pass("create database", options.db);

    if (!create_table(&context, &error)) {
        print_fail("create table", error);
        return false;
    }
    print_pass("create table", fmt::format("{}.{}", options.db, options.table));

    if (!wait_table_normal(&context)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!seed_data(&client, options)) {
        return false;
    }

    print_pass_bold("e2e restart persistence seed passed");
    return true;
}

bool run_restart_persistence_verify_case(const Options& options) {
    fmt::print("{} {}\n",
               colorize(COLOR::BOLD, "AdvisKV E2E Restart Persistence Verify"),
               colorize(COLOR::BLUE, "(read old data after full restart)"));

    if (!validate_options(options)) {
        return false;
    }

    E2EContext context(options);
    if (!wait_table_normal(&context)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!verify_seeded_data(&client, options)) {
        return false;
    }

    if (!wait_status("sdk put after restart", options, [&]() {
            return client.put("restart-key-after-restart",
                              "restart-value-after-restart");
        })) {
        return false;
    }

    print_pass_bold("e2e restart persistence verify passed");
    return true;
}

}  // namespace adviskv::e2e
