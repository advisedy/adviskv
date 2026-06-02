#pragma once

#include <cstdint>
#include <string>

namespace adviskv::bench {

struct BenchOptions {
    std::string meta_host{"127.0.0.1"};
    int32_t meta_port{50048};

    std::string sdm_host{"127.0.0.1"};
    int32_t sdm_port{50049};

    std::string db{"bench_db"};
    std::string table{"bench_table"};
    std::string zone{"dc1"};
    std::string resource_pool{"default"};
    int32_t shard_count{1};
    int32_t replica_count{3};

    // put get mixed 只有这三个
    std::string workload{"put"};
    double read_ratio{0.8};  // 只针对mixed场景

    int32_t threads{1};
    int64_t requests{1000};
    int64_t key_count{1000};
    int32_t value_size{128};
    int64_t warmup_requests{0};

    int32_t timeout_ms{60000};
    int32_t sdk_timeout_ms{3000};

    std::string output_json;

    std::string metrics_http_host{"127.0.0.1"};
    int32_t metrics_http_port{0};
    std::string metrics_http_path{"/metrics"};
    int32_t metrics_hold_seconds{0};
};

}  // namespace adviskv::bench