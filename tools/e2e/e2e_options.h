#pragma once

#include <cstdint>
#include <string>

#include "sdk/config.h"

namespace adviskv::e2e {

struct Options {
    std::string case_name{"basic_kv"};
    std::string meta_host{"127.0.0.1"};
    int32_t meta_port{50048};
    std::string sdm_host{"127.0.0.1"};
    int32_t sdm_port{50049};
    std::string db{"e2e_db"};
    std::string table{"e2e_table"};
    std::string zone{"dc1"};
    std::string resource_pool{"default"};
    int32_t shard_count{1};
    int32_t replica_count{3};
    int32_t key_count{8};
    int32_t table_count{100};       // sdm并发测试创建多少张表
    int32_t concurrency{4};        // sdm并发测试的worker数量
    int32_t timeout_ms{60000};      // 等待类操作的总超时时间
    int32_t poll_interval_ms{500};  // 两次重试之间sleep多久。
    int32_t sdk_timeout_ms{3000};   // sdk的内部一次RPC的请求超时
    sdk::LogLevel sdk_log_level{sdk::LogLevel::INFO};
    bool enable_sdk_log_callback{true};  // 是否允许sdk内部的日志输出
};

}  // namespace adviskv::e2e