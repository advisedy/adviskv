#include <fmt/core.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "arg_parser.h"
#include "bench_options.h"
#include "common/define.h"
#include "common/metrics/latency_recorder.h"
#include "common/status.h"
#include "common/type.h"
#include "e2e_context.h"
#include "e2e_options.h"
#include "e2e_table_util.h"
#include "sdk/client.h"

namespace adviskv::bench {
namespace {

using Clock = std::chrono::steady_clock;

struct BenchStats {
    std::atomic<int64_t> success{0};
    std::atomic<int64_t> failure{0};
    LatencyRecorder latency;
};

struct BenchResult {
    int64_t requests{0};
    int64_t success{0};
    int64_t failure{0};
    double elapsed_s{0.0};
    double attempted_qps{0.0};
    double success_qps{0.0};
    double failure_rate{0.0};
    LatencyRecorder::LatencySummary latency;
};

bool parse_args(int argc, char** argv, BenchOptions* options) {
    tools::ArgParser parser;
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
    parser.add_string("workload", options->workload);
    parser.add_double("read_ratio", options->read_ratio);
    parser.add_int32("threads", options->threads);
    parser.add_int64("requests", options->requests);
    parser.add_int64("key_count", options->key_count);
    parser.add_int32("value_size", options->value_size);
    parser.add_int64("warmup_requests", options->warmup_requests);
    parser.add_int32("timeout_ms", options->timeout_ms);
    parser.add_int32("sdk_timeout_ms", options->sdk_timeout_ms);
    parser.add_int32("route_cache_ttl_ms", options->route_cache_ttl_ms);
    parser.add_int32("route_cache_capacity", options->route_cache_capacity);
    parser.add_string("output_json", options->output_json);
    return parser.parse(argc, argv);
}

bool validate_options(const BenchOptions& options) {
    if (options.workload != "put" && options.workload != "get" &&
        options.workload != "mixed") {
        fmt::print(stderr, "--workload must be put, get, or mixed\n");
        return false;
    }
    if (options.threads <= 0 || options.requests < 0 ||
        options.key_count <= 0 || options.value_size < 0 ||
        options.warmup_requests < 0) {
        fmt::print(
            stderr,
            "--threads and --key_count must be positive; --requests, "
            "--value_size, and --warmup_requests must be non-negative\n");
        return false;
    }
    if (options.read_ratio < 0.0 || options.read_ratio > 1.0) {
        fmt::print(stderr, "--read_ratio must be in [0.0, 1.0]\n");
        return false;
    }
    if (options.meta_port <= 0 || options.sdm_port <= 0 ||
        options.timeout_ms <= 0 || options.shard_count <= 0 ||
        options.replica_count < 0 || options.sdk_timeout_ms <= 0 ||
        options.route_cache_ttl_ms < 0 || options.route_cache_capacity <= 0) {
        fmt::print(stderr,
                   "ports, timeout, shard/replica counts, and route cache "
                   "config invalid\n");
        return false;
    }
    if (options.db.empty() || options.table.empty() || options.zone.empty() ||
        options.resource_pool.empty()) {
        fmt::print(stderr,
                   "--db, --table, --zone, and --resource_pool must not be "
                   "empty\n");
        return false;
    }
    if (options.key_count > std::numeric_limits<int32_t>::max()) {
        fmt::print(stderr, "--key_count is too large\n");
        return false;
    }
    return true;
}

e2e::Options to_e2e_options(const BenchOptions& options) {
    e2e::Options e2e_options;
    e2e_options.meta_host = options.meta_host;
    e2e_options.meta_port = options.meta_port;
    e2e_options.sdm_host = options.sdm_host;
    e2e_options.sdm_port = options.sdm_port;
    e2e_options.db = options.db;
    e2e_options.table = options.table;
    e2e_options.zone = options.zone;
    e2e_options.resource_pool = options.resource_pool;
    e2e_options.shard_count = options.shard_count;
    e2e_options.replica_count = options.replica_count;
    e2e_options.key_count = static_cast<int32_t>(options.key_count);
    e2e_options.timeout_ms = options.timeout_ms;
    e2e_options.sdk_timeout_ms = options.sdk_timeout_ms;
    e2e_options.route_cache_ttl_ms = options.route_cache_ttl_ms;
    e2e_options.route_cache_capacity = options.route_cache_capacity;
    e2e_options.sdk_log_level = sdk::LogLevel::WARN;
    e2e_options.enable_sdk_log_callback = false;
    return e2e_options;
}

std::string make_key(int64_t index) {
    return fmt::format("bench-key-{:012d}", index);
}

std::string make_value(const BenchOptions& options, int64_t index) {
    std::string value = fmt::format("bench-value-{:012d}", index);
    if (static_cast<int32_t>(value.size()) < options.value_size) {
        value.resize(static_cast<size_t>(options.value_size), 'x');
    } else if (static_cast<int32_t>(value.size()) > options.value_size) {
        value.resize(static_cast<size_t>(options.value_size));
    }
    return value;
}

sdk::KVClient make_client(const BenchOptions& options) {
    sdk::KVClientConf conf;
    conf.db_name = options.db;
    conf.table_name = options.table;
    conf.sdm_host = options.sdm_host;
    conf.sdm_port = options.sdm_port;
    conf.sdm_timeout_ms = options.sdk_timeout_ms;
    conf.storage_timeout_ms = options.sdk_timeout_ms;
    conf.route_cache_ttl_ms = options.route_cache_ttl_ms;
    conf.route_cache_capacity =
        static_cast<size_t>(options.route_cache_capacity);
    conf.log.level = sdk::LogLevel::WARN;
    return sdk::KVClient(conf);
}

bool prepare_bench_table(const BenchOptions& options) {
    e2e::Options e2e_options = to_e2e_options(options);
    e2e::E2EContext context(e2e_options);
    return e2e::prepare_table(&context);
}

bool prepare_read_dataset(const BenchOptions& options) {
    sdk::KVClient client = make_client(options);
    for (int64_t i = 0; i < options.key_count; ++i) {
        Status status = client.put(make_key(i), make_value(options, i));
        if (status.fail()) {
            fmt::print(stderr, "prepare put failed, key={}, status={}\n",
                       make_key(i), status.to_string());
            return false;
        }
    }
    fmt::print("prepared readable dataset: keys={}\n", options.key_count);
    return true;
}

bool should_read(const BenchOptions& options, std::mt19937_64& rng) {
    if (options.workload == "get") {
        return true;
    }
    if (options.workload == "put") {
        return false;
    }
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng) < options.read_ratio;
}

Status execute_one(sdk::KVClient* client, const BenchOptions& options,
                   int64_t request_id, bool read_request) {
    int64_t key_index = request_id % options.key_count;
    Key key = make_key(key_index);
    if (read_request) {
        Value value;
        return client->get(key, &value);
    }
    return client->put(key, make_value(options, request_id));
}

void run_worker(const BenchOptions& options, std::atomic<int64_t>* next_request,
                int64_t request_count, BenchStats* stats, bool record_stats,
                int32_t worker_id) {
    sdk::KVClient client = make_client(options);
    std::mt19937_64 rng(static_cast<uint64_t>(
        Clock::now().time_since_epoch().count() + worker_id));

    while (true) {
        int64_t request_id = next_request->fetch_add(1);
        if (request_id >= request_count) {
            break;
        }

        bool read_request = should_read(options, rng);
        auto start = Clock::now();
        Status status = execute_one(&client, options, request_id, read_request);
        if (!record_stats) {
            continue;
        }
        auto end = Clock::now();
        int64_t latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count();
        stats->latency.record(latency_us);
        if (status.ok()) {
            stats->success.fetch_add(1);
        } else {
            stats->failure.fetch_add(1);
        }
    }
}

BenchResult run_phase(const BenchOptions& options, int64_t request_count,
                      bool record_stats) {
    if (request_count <= 0) {
        return BenchResult{};
    }
    BenchStats stats;
    std::atomic<int64_t> next_request{0};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(options.threads));

    auto start = Clock::now();
    for (int32 i = 0; i < options.threads; ++i) {
        int32 worker_id = i;
        workers.emplace_back([&, worker_id]() -> void {
            run_worker(options, &next_request, request_count, &stats,
                       record_stats, worker_id);
        });
    }
    for (std::thread& worker : workers) {
        worker.join();
    }
    auto end = Clock::now();

    BenchResult result;
    result.requests = request_count;
    result.success = stats.success.load();
    result.failure = stats.failure.load();
    result.elapsed_s = std::chrono::duration<double>(end - start).count();
    if (result.elapsed_s > 0.0) {
        result.attempted_qps = to<double>(request_count) / result.elapsed_s;
        result.success_qps = to<double>(result.success) / result.elapsed_s;
    }
    result.failure_rate =
        to<double>(result.failure) / to<double>(request_count);
    result.latency = stats.latency.summary();
    return result;
}

bool write_json(const BenchOptions& options, const BenchResult& result) {
    if (options.output_json.empty()) {
        return true;
    }
    std::ofstream out(options.output_json);
    if (!out) {
        fmt::print(stderr, "failed to open output_json '{}'\n",
                   options.output_json);
        return false;
    }
    out << "{\n"
        << fmt::format("  \"workload\": \"{}\",\n", options.workload)
        << fmt::format("  \"threads\": {},\n", options.threads)
        << fmt::format("  \"key_count\": {},\n", options.key_count)
        << fmt::format("  \"value_size\": {},\n", options.value_size)
        << fmt::format("  \"read_ratio\": {:.6f},\n", options.read_ratio)
        << fmt::format("  \"warmup_requests\": {},\n", options.warmup_requests)
        << fmt::format("  \"requests\": {},\n", result.requests)
        << fmt::format("  \"success\": {},\n", result.success)
        << fmt::format("  \"failure\": {},\n", result.failure)
        << fmt::format("  \"elapsed_s\": {:.6f},\n", result.elapsed_s)
        << fmt::format("  \"attempted_qps\": {:.6f},\n", result.attempted_qps)
        << fmt::format("  \"success_qps\": {:.6f},\n", result.success_qps)
        << fmt::format("  \"failure_rate\": {:.6f},\n", result.failure_rate)
        << "  \"latency_us\": {\n"
        << fmt::format("    \"count\": {},\n", result.latency.count)
        << fmt::format("    \"min\": {},\n", result.latency.min_us)
        << fmt::format("    \"avg\": {:.3f},\n", result.latency.avg_us)
        << fmt::format("    \"p50\": {},\n", result.latency.p50_us)
        << fmt::format("    \"p95\": {},\n", result.latency.p95_us)
        << fmt::format("    \"p99\": {},\n", result.latency.p99_us)
        << fmt::format("    \"max\": {}\n", result.latency.max_us) << "  }\n"
        << "}\n";
    return true;
}

void print_result(const BenchOptions& options, const BenchResult& result) {
    fmt::print(
        "benchmark result\n"
        "  workload: {}\n"
        "  threads: {}\n"
        "  requests: {}\n"
        "  success: {}\n"
        "  failure: {}\n"
        "  failure_rate: {:.4f}\n"
        "  elapsed_s: {:.6f}\n"
        "  attempted_qps: {:.2f}\n"
        "  success_qps: {:.2f}\n"
        "  min_us: {}\n"
        "  avg_us: {:.2f}\n"
        "  p50_us: {}\n"
        "  p95_us: {}\n"
        "  p99_us: {}\n"
        "  max_us: {}\n",
        options.workload, options.threads, result.requests, result.success,
        result.failure, result.failure_rate, result.elapsed_s,
        result.attempted_qps, result.success_qps, result.latency.min_us,
        result.latency.avg_us, result.latency.p50_us, result.latency.p95_us,
        result.latency.p99_us, result.latency.max_us);
}

}  // namespace
}  // namespace adviskv::bench

int main(int argc, char** argv) {
    adviskv::bench::BenchOptions options;

    if (!adviskv::bench::parse_args(argc, argv, &options)) return 1;

    if (!adviskv::bench::validate_options(options)) {
        return 1;
    }

    if (!adviskv::bench::prepare_bench_table(options)) {
        return 1;
    }

    if ((options.workload == "get" || options.workload == "mixed")) {
        if (!adviskv::bench::prepare_read_dataset(options)) {
            return 1;
        }
    }

    if (options.warmup_requests > 0) {
        fmt::print("warmup start: requests={}\n", options.warmup_requests);
        IGNORE_RESULT(
            adviskv::bench::run_phase(options, options.warmup_requests, false));
        fmt::print("warmup done\n");
    }

    adviskv::bench::BenchResult result =
        adviskv::bench::run_phase(options, options.requests, true);
    adviskv::bench::print_result(options, result);
    if (!adviskv::bench::write_json(options, result)) {
        return 1;
    }
    return 0;
}