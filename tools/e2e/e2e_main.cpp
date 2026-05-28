#include <fmt/core.h>

#include <exception>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "cases/basic_kv_case.h"
#include "cases/follower_log_catchup_case.h"
#include "cases/follower_snapshot_catchup_case.h"
#include "cases/leader_failover_case.h"
#include "cases/restart_persistence_case.h"
#include "common/confmgr.h"
#include "common/log.h"
#include "common/path_util.h"
#include "e2e_assert.h"
#include "e2e_options.h"

namespace {

void init_conf(const char* conf_file) {
    const std::filesystem::path path{conf_file};
    adviskv::ConfMgr::get_instance().LoadFromFile(
        adviskv::path_from_project_root(path).string());
}

void init_e2e_logger() {
    adviskv::LogConfig config;
    config.logger_name = CONF_GET_STR("logger_name");
    config.log_dir = adviskv::path_from_config("log_dir").string();
    config.log_filename = CONF_GET_STR("log_filename");
    config.log_level = CONF_GET_STR("log_level");
    config.log_to_console = CONF_GET_BOOL("log_to_console");
    config.log_to_file = CONF_GET_BOOL("log_to_file");
    adviskv::Logger::get_instance().init(config);
    LOG_INFO("e2e logger initialized, log_dir={}, log_filename={}",
             config.log_dir, config.log_filename);
}

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

adviskv::e2e::Options parse_options(int argc, char** argv,
                                    int first_option_arg) {
    adviskv::e2e::Options options;
    for (int i = first_option_arg; i < argc; ++i) {
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

#define RUN_CASE(case_literal, fn)                       \
    if (options.case_name == case_literal) {             \
        adviskv::e2e::print_case_start(case_literal);    \
        const bool ok = fn(options);                     \
        if (ok) {                                        \
            adviskv::e2e::print_case_pass(case_literal); \
        } else {                                         \
            adviskv::e2e::print_case_fail(case_literal); \
        }                                                \
        return ok;                                       \
    }

bool run_case(const adviskv::e2e::Options& options) {
    RUN_CASE("basic_kv", adviskv::e2e::run_basic_kv_case)
    RUN_CASE("restart_persistence_seed",
             adviskv::e2e::run_restart_persistence_seed_case)
    RUN_CASE("restart_persistence_verify",
             adviskv::e2e::run_restart_persistence_verify_case)
    RUN_CASE("leader_failover_seed",
             adviskv::e2e::run_leader_failover_seed_case)
    RUN_CASE("leader_failover_verify",
             adviskv::e2e::run_leader_failover_verify_case)
    RUN_CASE("follower_log_catchup_prepare",
             adviskv::e2e::run_follower_log_catchup_prepare_case)
    RUN_CASE("follower_log_catchup_write_gap",
             adviskv::e2e::run_follower_log_catchup_write_gap_case)
    RUN_CASE("follower_log_catchup_verify",
             adviskv::e2e::run_follower_log_catchup_verify_case)
    RUN_CASE("follower_snapshot_catchup_prepare",
             adviskv::e2e::run_follower_snapshot_catchup_prepare_case)
    RUN_CASE("follower_snapshot_catchup_write_gap",
             adviskv::e2e::run_follower_snapshot_catchup_write_gap_case)
    RUN_CASE("follower_snapshot_catchup_verify",
             adviskv::e2e::run_follower_snapshot_catchup_verify_case)
    adviskv::e2e::print_fail(
        "select case", fmt::format("unknown case '{}'", options.case_name));
    return false;
}

#undef RUN_CASE

}  // namespace

int main(int argc, char** argv) {
    try {
        const bool has_conf_arg = argc >= 2 && argv[1][0] != '-';
        if (!has_conf_arg) return 1;
        const char* conf_file = argv[1];
        init_conf(conf_file);
        init_e2e_logger();
        const adviskv::e2e::Options options =
            parse_options(argc, argv, has_conf_arg ? 2 : 1);
        return run_case(options) ? 0 : 1;
    } catch (const std::exception& e) {
        adviskv::e2e::print_fail("fatal", e.what());
        return 1;
    }
}