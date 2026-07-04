#pragma once

#include <fmt/core.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
#include "e2e_route_util.h"
#include "e2e_table_util.h"
#include "heavy_case_util.h"
#include "sdk/client.h"
#include "sdk/config.h"

namespace adviskv::e2e {

namespace resize_resilience_detail {

constexpr const char* kBasePrefix = "resize-resilience";
constexpr const char* kAfterPrefix = "resize-resilience-after";
constexpr const char* kLivePrefix = "resize-live";
constexpr int32_t kWriterSleepMs = 30;

inline std::string first_key(const std::string& prefix) {
    return make_case_kvs(prefix, 1).front().first;
}

class ContinuousWriter {
public:
    explicit ContinuousWriter(const Options& options) : options_(options) {}

    ~ContinuousWriter() { stop_and_join(); }

    void start() {
        worker_ = std::thread([this]() { run(); });
    }

    void stop_and_join() {
        stop_.store(true, std::memory_order_relaxed);
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    std::vector<KV> acknowledged() const {
        std::lock_guard lock(mutex_);
        return acknowledged_;
    }

    int32_t succeeded() const {
        return succeeded_.load(std::memory_order_relaxed);
    }

    int32_t failed() const { return failed_.load(std::memory_order_relaxed); }

private:
    void run() {
        Options writer_options = options_;
        writer_options.enable_sdk_log_callback = false;
        writer_options.sdk_log_level = sdk::LogLevel::WARN;
        sdk::KVClient client = make_kv_client(writer_options);

        int32_t index = 0;
        while (!stop_.load(std::memory_order_relaxed)) {
            KV kv{fmt::format("{}-key-{:06d}", kLivePrefix, index),
                  fmt::format("{}-value-{:06d}", kLivePrefix, index)};
            ++index;

            const Status status = client.put(kv.first, kv.second);
            if (status.ok()) {
                {
                    std::lock_guard lock(mutex_);
                    acknowledged_.push_back(std::move(kv));
                }
                succeeded_.fetch_add(1, std::memory_order_relaxed);
            } else {
                failed_.fetch_add(1, std::memory_order_relaxed);
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(kWriterSleepMs));
        }
    }

    Options options_;
    std::atomic<bool> stop_{false};
    std::atomic<int32_t> succeeded_{0};
    std::atomic<int32_t> failed_{0};
    mutable std::mutex mutex_;
    std::vector<KV> acknowledged_;
    std::thread worker_;
};

inline bool alter_and_wait(E2EContext* context, int32_t replica_count) {
    std::string error;
    if (!alter_table_replica_count(context, replica_count, &error)) {
        print_fail("alter table replica_count", error);
        return false;
    }
    print_pass("alter table replica_count",
               fmt::format("target={}", replica_count));
    return wait_table_replica_count(context, replica_count);
}

inline bool wait_route_replica_count(E2EContext* context,
                                     const std::string& key,
                                     int32_t replica_count) {
    const Options& options = context->options();
    std::string last_error;
    return eventually(
        fmt::format("route replica_count {}", replica_count), options,
        [&]() {
            sdk::RouteInfo route;
            std::string error;
            if (!get_route(context, key, &route, &error)) {
                return CheckResult::fail(error);
            }
            if (static_cast<int32_t>(route.replicas.size()) !=
                replica_count) {
                return CheckResult::fail(fmt::format(
                    "route_replicas={}, expected={}", route.replicas.size(),
                    replica_count));
            }
            return CheckResult::pass(
                fmt::format("route_replicas={}", route.replicas.size()));
        },
        &last_error);
}

}  // namespace resize_resilience_detail

// 扩缩容期间持续写入：
// 后台线程持续 put 新 key，主线程依次执行 3->4、4->5、5->3。
// 最后校验所有已经返回 OK 的写入都能读到，证明 resize 期间不会丢已确认写。
inline bool run_table_replica_count_resize_with_writes_case(
    const Options& options) {
    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }
    if (!wait_table_replica_count(&context, options.replica_count)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!write_dataset(&client, options,
                       resize_resilience_detail::kBasePrefix)) {
        return false;
    }
    if (!verify_dataset(&client, options,
                        resize_resilience_detail::kBasePrefix)) {
        return false;
    }

    resize_resilience_detail::ContinuousWriter writer(options);
    writer.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const int32_t initial_count = options.replica_count;
    const int32_t first_expand = initial_count + 1;
    const int32_t second_expand = initial_count + 2;

    if (!resize_resilience_detail::alter_and_wait(&context, first_expand)) {
        return false;
    }
    if (!resize_resilience_detail::alter_and_wait(&context, second_expand)) {
        return false;
    }
    if (!resize_resilience_detail::alter_and_wait(&context, initial_count)) {
        return false;
    }

    writer.stop_and_join();
    const std::vector<KV> acknowledged = writer.acknowledged();
    if (acknowledged.empty()) {
        print_fail("continuous writer", "no acknowledged writes");
        return false;
    }
    print_pass("continuous writer",
               fmt::format("acknowledged={}, failed={}", acknowledged.size(),
                           writer.failed()));

    if (!verify_dataset(&client, options,
                        resize_resilience_detail::kBasePrefix)) {
        return false;
    }
    if (!verify_kvs(&client, options, resize_resilience_detail::kLivePrefix,
                    acknowledged)) {
        return false;
    }

    return write_dataset(&client, options,
                         resize_resilience_detail::kAfterPrefix) &&
           verify_dataset(&client, options,
                          resize_resilience_detail::kAfterPrefix);
}

// resize 故障/重启类场景的准备阶段：
// 建表、写入基准数据，并输出当前 route，Python 侧据此选择一个已在 group 内的节点。
inline bool run_table_replica_count_resize_disruption_prepare_case(
    const Options& options) {
    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }
    if (!wait_table_replica_count(&context, options.replica_count)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!write_dataset(&client, options,
                       resize_resilience_detail::kBasePrefix)) {
        return false;
    }
    if (!verify_dataset(&client, options,
                        resize_resilience_detail::kBasePrefix)) {
        return false;
    }

    const std::string probe_key =
        resize_resilience_detail::first_key(resize_resilience_detail::kBasePrefix);
    if (!wait_route_has_leader(&context, probe_key, nullptr)) {
        return false;
    }
    return heavy::print_route_replicas(&context, probe_key);
}

// 只发起 alter，不等待收敛。这个 case 专门给 Python 在 resize 中间插入 kill/restart。
inline bool run_table_replica_count_resize_start_alter_case(
    const Options& options) {
    E2EContext context(options);
    std::string error;
    if (!alter_table_replica_count(&context, options.replica_count, &error)) {
        print_fail("alter table replica_count", error);
        return false;
    }
    print_pass("alter table replica_count",
               fmt::format("target={}", options.replica_count));
    return true;
}

// resize 故障/重启类场景的验证阶段：
// 等待目标 replica_count 收敛，验证 alter 前的数据仍可读，并再次写读一批新数据。
inline bool run_table_replica_count_resize_disruption_verify_case(
    const Options& options) {
    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!wait_table_replica_count(&context, options.replica_count)) {
        return false;
    }
    const std::string probe_key =
        resize_resilience_detail::first_key(resize_resilience_detail::kBasePrefix);
    if (!wait_route_has_leader(&context, probe_key, nullptr)) {
        return false;
    }
    if (!resize_resilience_detail::wait_route_replica_count(
            &context, probe_key, options.replica_count)) {
        return false;
    }
    if (!heavy::print_route_replicas(&context, probe_key)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!verify_dataset(&client, options,
                        resize_resilience_detail::kBasePrefix)) {
        return false;
    }
    return write_dataset(&client, options,
                         resize_resilience_detail::kAfterPrefix) &&
           verify_dataset(&client, options,
                          resize_resilience_detail::kAfterPrefix);
}

}  // namespace adviskv::e2e
