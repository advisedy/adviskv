#include <fmt/core.h>

#include <exception>
#include <stdexcept>
#include <string>

#include "cases/basic_kv_case.h"
#include "cases/restart_persistence_case.h"
#include "e2e_assert.h"
#include "e2e_options.h"

namespace {

std::string value_after_equals(const std::string& arg,
                               const std::string& flag_name) {
    const std::string prefix = flag_name + "=";
    if (arg.rfind(prefix, 0) == 0) {
        return arg.substr(prefix.size());
    }
    return {};
}

bool parse_int_flag(const std::string& arg, const std::string& flag_name,
                    int32_t* output) {
    const std::string value = value_after_equals(arg, flag_name);
    if (value.empty()) {
        return false;
    }
    *output = std::stoi(value);
    return true;
}

adviskv::e2e::Options parse_options(int argc, char** argv) {
    adviskv::e2e::Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (const std::string value = value_after_equals(arg, "--case");
            !value.empty()) {
            options.case_name = value;
        } else if (const std::string value =
                       value_after_equals(arg, "--meta_host");
                   !value.empty()) {
            options.meta_host = value;
        } else if (parse_int_flag(arg, "--meta_port", &options.meta_port)) {
        } else if (const std::string value =
                       value_after_equals(arg, "--sdm_host");
                   !value.empty()) {
            options.sdm_host = value;
        } else if (parse_int_flag(arg, "--sdm_port", &options.sdm_port)) {
        } else if (const std::string value = value_after_equals(arg, "--db");
                   !value.empty()) {
            options.db = value;
        } else if (const std::string value = value_after_equals(arg, "--table");
                   !value.empty()) {
            options.table = value;
        } else if (const std::string value = value_after_equals(arg, "--zone");
                   !value.empty()) {
            options.zone = value;
        } else if (const std::string value =
                       value_after_equals(arg, "--resource_pool");
                   !value.empty()) {
            options.resource_pool = value;
        } else if (parse_int_flag(arg, "--shard_count", &options.shard_count)) {
        } else if (parse_int_flag(arg, "--replica_count",
                                  &options.replica_count)) {
        } else if (parse_int_flag(arg, "--key_count", &options.key_count)) {
        } else if (parse_int_flag(arg, "--timeout_ms", &options.timeout_ms)) {
        } else if (parse_int_flag(arg, "--poll_interval_ms",
                                  &options.poll_interval_ms)) {
        } else {
            throw std::invalid_argument(
                fmt::format("unknown argument: {}", arg));
        }
    }
    return options;
}

bool run_case(const adviskv::e2e::Options& options) {
    if (options.case_name == "basic_kv") {
        return adviskv::e2e::run_basic_kv_case(options);
    }
    if (options.case_name == "restart_persistence_seed") {
        return adviskv::e2e::run_restart_persistence_seed_case(options);
    }
    if (options.case_name == "restart_persistence_verify") {
        return adviskv::e2e::run_restart_persistence_verify_case(options);
    }
    adviskv::e2e::print_fail(
        "select case", fmt::format("unknown case '{}'", options.case_name));
    return false;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const adviskv::e2e::Options options = parse_options(argc, argv);
        return run_case(options) ? 0 : 1;
    } catch (const std::exception& e) {
        adviskv::e2e::print_fail("fatal", e.what());
        return 1;
    }
}