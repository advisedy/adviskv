#pragma once

#include <fmt/core.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
#include "e2e_table_util.h"
#include "sdk/client.h"

namespace adviskv::e2e {

namespace many_tables_detail {

inline Options table_options(const Options& base, int32_t index) {
    Options one = base;
    one.table = fmt::format("{}_{:04d}", base.table, index);
    one.key_count = 1;
    one.enable_sdk_log_callback = false;
    one.sdk_log_level = sdk::LogLevel::WARN;
    return one;
}

inline bool run_parallel_stage(
    const Options& options, const std::string& stage, int32_t count,
    const std::function<bool(int32_t, std::string*)>& fn) {
    if (count <= 0) {
        print_fail(stage, "count must be positive");
        return false;
    }
    if (options.concurrency <= 0) {
        print_fail(stage, "concurrency must be positive");
        return false;
    }

    const int32_t worker_count = std::min(options.concurrency, count);
    std::atomic<int32_t> next{0};
    std::atomic<bool> failed{false};
    std::mutex error_mutex;
    std::string first_error;
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(worker_count));

    for (int32_t worker = 0; worker < worker_count; ++worker) {
        workers.emplace_back([&]() {
            while (!failed.load()) {
                int32_t index = next.fetch_add(1);
                if (index >= count) {
                    return;
                }

                std::string error;
                if (fn(index, &error)) {
                    continue;
                }

                {
                    std::lock_guard lock(error_mutex);
                    if (first_error.empty()) {
                        first_error =
                            fmt::format("index={}, {}", index, error);
                    }
                }
                failed.store(true);
                return;
            }
        });
    }

    for (std::thread& worker : workers) {
        worker.join();
    }

    if (failed.load()) {
        print_fail(stage, first_error);
        return false;
    }

    print_pass(stage,
               fmt::format("count={}, concurrency={}", count, worker_count));
    return true;
}

inline bool create_one_table(const Options& base, int32_t index,
                             std::string* error) {
    Options one = table_options(base, index);
    E2EContext context(one);
    if (!create_table(&context, error)) {
        return false;
    }
    return true;
}

inline bool wait_one_table_normal(const Options& base, int32_t index,
                                  std::string* error) {
    Options one = table_options(base, index);
    E2EContext context(one);
    if (!wait_table_normal(&context)) {
        *error = fmt::format("table {} did not become NORMAL", one.table);
        return false;
    }
    return true;
}

inline bool verify_one_table_kv(const Options& base, int32_t index,
                                std::string* error) {
    Options one = table_options(base, index);
    sdk::KVClient client = make_kv_client(one);
    const std::string key = fmt::format("many-tables-key-{:04d}", index);
    const std::string value = fmt::format("many-tables-value-{:04d}", index);

    if (!wait_status(fmt::format("put {}", one.table), one,
                     [&]() { return client.put(key, value); })) {
        *error = fmt::format("put failed for table {}", one.table);
        return false;
    }
    if (!wait_get_value(&client, one, key, value)) {
        *error = fmt::format("get failed for table {}", one.table);
        return false;
    }
    return true;
}

inline bool drop_one_table(const Options& base, int32_t index,
                           std::string* error) {
    Options one = table_options(base, index);
    E2EContext context(one);
    if (!drop_table(&context, error)) {
        return false;
    }
    return true;
}

inline bool wait_one_table_deleted(const Options& base, int32_t index,
                                   std::string* error) {
    Options one = table_options(base, index);
    E2EContext context(one);
    if (!wait_table_deleted(&context)) {
        *error = fmt::format("table {} did not become deleted", one.table);
        return false;
    }
    return true;
}

}  // namespace many_tables_detail

// 控制面并发正确性测试：
// 并发创建多张 table，等待全部 NORMAL，对每张表写读一个 key，
// 再并发 drop 并等待全部删除。默认规模适合本地/e2e，手动可把 table_count 放大。
inline bool run_many_tables_create_drop_case(const Options& options) {
    if (options.table_count <= 0) {
        print_fail("validate options", "--table_count must be positive");
        return false;
    }
    if (options.concurrency <= 0) {
        print_fail("validate options", "--concurrency must be positive");
        return false;
    }

    E2EContext context(options);
    std::string error;
    if (!create_db(&context, &error)) {
        print_fail("create database", error);
        return false;
    }
    print_pass("create database", options.db);

    using namespace many_tables_detail;
    const int32_t count = options.table_count;

    if (!run_parallel_stage(options, "create tables", count,
                            [&](int32_t index, std::string* stage_error) {
                                return create_one_table(options, index,
                                                        stage_error);
                            })) {
        return false;
    }
    if (!run_parallel_stage(options, "wait tables normal", count,
                            [&](int32_t index, std::string* stage_error) {
                                return wait_one_table_normal(options, index,
                                                             stage_error);
                            })) {
        return false;
    }
    if (!run_parallel_stage(options, "verify table kv", count,
                            [&](int32_t index, std::string* stage_error) {
                                return verify_one_table_kv(options, index,
                                                           stage_error);
                            })) {
        return false;
    }
    if (!run_parallel_stage(options, "drop tables", count,
                            [&](int32_t index, std::string* stage_error) {
                                return drop_one_table(options, index,
                                                      stage_error);
                            })) {
        return false;
    }
    if (!run_parallel_stage(options, "wait tables deleted", count,
                            [&](int32_t index, std::string* stage_error) {
                                return wait_one_table_deleted(options, index,
                                                              stage_error);
                            })) {
        return false;
    }

    return true;
}

}  // namespace adviskv::e2e
