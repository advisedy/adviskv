#include <exception>
#include <filesystem>
#include <string>

#include <fmt/core.h>

#include "cases/basic_kv_case.h"
#include "cases/create_table_crash_recovery_case.h"
#include "cases/follower_log_catchup_case.h"
#include "cases/follower_snapshot_catchup_case.h"
#include "cases/leader_failover_case.h"
#include "cases/many_tables_create_drop_case.h"
#include "cases/meta_crash_recovery_case.h"
#include "cases/restart_persistence_case.h"
#include "cases/scale_to_zero_case.h"
#include "cases/sdm_crash_recovery_case.h"
#include "cases/table_replica_count_resize_case.h"
#include "cases/table_replica_count_resize_resilience_case.h"
#include "common/arg_parser.h"
#include "common/confmgr.h"
#include "common/log.h"
#include "common/path_util.h"
#include "e2e_assert.h"
#include "e2e_options.h"

namespace {

void init_conf(const char* conf_file) {
    const std::filesystem::path path{conf_file};
    adviskv::ConfMgr::get_instance().LoadFromFile(adviskv::path_from_project_root(path).string());
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
    LOG_INFO("e2e logger initialized, log_dir={}, log_filename={}", config.log_dir, config.log_filename);
}

bool parse_options(int argc, char** argv, std::string* conf_file, adviskv::e2e::Options* options) {
    adviskv::ArgParser parser;
    parser.add_string("conf", *conf_file);
    parser.add_string("case", options->case_name);
    parser.add_string("meta_host", options->meta_host);
    parser.add_int32("meta_port", options->meta_port);
    parser.add_string("sdm_host", options->sdm_host);
    parser.add_int32("sdm_port", options->sdm_port);
    parser.add_string("db", options->db);
    parser.add_string("table", options->table);
    parser.add_string("zone", options->zone);
    parser.add_string("resource_pool", options->resource_pool);
    parser.add_int32("shard_count", options->shard_count);
    parser.add_int32("replica_count", options->replica_count);
    parser.add_int32("key_count", options->key_count);
    parser.add_int32("table_count", options->table_count);
    parser.add_int32("concurrency", options->concurrency);
    parser.add_int32("timeout_ms", options->timeout_ms);
    parser.add_int32("poll_interval_ms", options->poll_interval_ms);
    return parser.parse(argc, argv);
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
    RUN_CASE("restart_persistence_seed", adviskv::e2e::run_restart_persistence_seed_case)
    RUN_CASE("restart_persistence_verify", adviskv::e2e::run_restart_persistence_verify_case)
    RUN_CASE("create_table_crash_before_persist_prepare",
             adviskv::e2e::run_create_table_crash_before_persist_prepare_case)
    RUN_CASE("create_table_crash_before_persist_verify",
             adviskv::e2e::run_create_table_crash_before_persist_verify_case)
    RUN_CASE("create_table_crash_after_persist_prepare",
             adviskv::e2e::run_create_table_crash_after_persist_prepare_case)
    RUN_CASE("create_table_crash_after_persist_verify", adviskv::e2e::run_create_table_crash_after_persist_verify_case)
    RUN_CASE("leader_failover_seed", adviskv::e2e::run_leader_failover_seed_case)
    RUN_CASE("leader_failover_verify", adviskv::e2e::run_leader_failover_verify_case)
    RUN_CASE("follower_log_catchup_prepare", adviskv::e2e::run_follower_log_catchup_prepare_case)
    RUN_CASE("follower_log_catchup_write_gap", adviskv::e2e::run_follower_log_catchup_write_gap_case)
    RUN_CASE("follower_log_catchup_verify", adviskv::e2e::run_follower_log_catchup_verify_case)
    RUN_CASE("follower_snapshot_catchup_prepare", adviskv::e2e::run_follower_snapshot_catchup_prepare_case)
    RUN_CASE("follower_snapshot_catchup_write_gap", adviskv::e2e::run_follower_snapshot_catchup_write_gap_case)
    RUN_CASE("follower_snapshot_catchup_verify", adviskv::e2e::run_follower_snapshot_catchup_verify_case)
    RUN_CASE("sdm_crash_seed", adviskv::e2e::run_sdm_crash_seed_case)
    RUN_CASE("sdm_crash_verify", adviskv::e2e::run_sdm_crash_verify_case)
    RUN_CASE("meta_crash_seed", adviskv::e2e::run_meta_crash_seed_case)
    RUN_CASE("meta_crash_verify", adviskv::e2e::run_meta_crash_verify_case)
    RUN_CASE("scale_to_zero", adviskv::e2e::run_scale_to_zero_case)
    RUN_CASE("table_replica_count_resize", adviskv::e2e::run_table_replica_count_resize_case)
    RUN_CASE("table_replica_count_resize_with_writes", adviskv::e2e::run_table_replica_count_resize_with_writes_case)
    RUN_CASE("table_replica_count_resize_disruption_prepare",
             adviskv::e2e::run_table_replica_count_resize_disruption_prepare_case)
    RUN_CASE("table_replica_count_resize_start_alter", adviskv::e2e::run_table_replica_count_resize_start_alter_case)
    RUN_CASE("table_replica_count_resize_disruption_verify",
             adviskv::e2e::run_table_replica_count_resize_disruption_verify_case)
    RUN_CASE("many_tables_create_drop", adviskv::e2e::run_many_tables_create_drop_case)
    adviskv::e2e::print_fail("select case", fmt::format("unknown case '{}'", options.case_name));
    return false;
}

#undef RUN_CASE

}  // namespace

int main(int argc, char** argv) {
    try {
        std::string conf_file;
        adviskv::e2e::Options options;
        if (!parse_options(argc, argv, &conf_file, &options)) {
            return 1;
        }
        if (conf_file.empty()) {
            fmt::print(stderr, "--conf must be provided\n");
            return 1;
        }
        init_conf(conf_file.c_str());
        init_e2e_logger();
        return run_case(options) ? 0 : 1;
    } catch (const std::exception& e) {
        adviskv::e2e::print_fail("fatal", e.what());
        return 1;
    }
}