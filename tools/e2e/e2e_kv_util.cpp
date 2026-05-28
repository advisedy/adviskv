#include "e2e_kv_util.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <string_view>
#include <thread>
#include <utility>

#include "common/log.h"
#include "common/type.h"
#include "e2e_assert.h"
#include "sdk/config.h"
#include "sdk/log.h"

namespace adviskv::e2e {

namespace {
using Clock = std::chrono::steady_clock;

constexpr int32_t kVerboseDatasetLogLimit = 32;
constexpr int32_t kDatasetLogRangeSize = 100;

bool wait_status_quiet(const std::string& name, const Options& options,
                       const std::function<Status()>& operation) {
    const auto deadline =
        Clock::now() + std::chrono::milliseconds(options.timeout_ms);
    Status last_status{StatusCode::ERROR, "not attempted"};
    while (Clock::now() < deadline) {
        last_status = operation();
        if (last_status.ok()) {
            return true;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(options.poll_interval_ms));
    }
    print_fail(name, fmt::format("timed out: {}", last_status.to_string()));
    return false;
}

bool wait_get_value_quiet(sdk::KVClient* client, const Options& options,
                          const std::string& key, const std::string& expected) {
    const auto deadline =
        Clock::now() + std::chrono::milliseconds(options.timeout_ms);
    std::string last_error = "not attempted";
    while (Clock::now() < deadline) {
        Value value;
        const Status status = client->get(key, &value);
        if (status.fail()) {
            last_error = status.to_string();
        } else if (value != expected) {
            last_error = fmt::format("unexpected value '{}', expected '{}'",
                                     value, expected);
        } else {
            return true;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(options.poll_interval_ms));
    }
    print_fail(fmt::format("sdk get {}", key),
               fmt::format("timed out: {}", last_error));
    return false;
}

std::string kv_range_message(const std::vector<KV>& kvs, size_t begin,
                             size_t end) {
    return fmt::format("index=[{}..{}], keys=[{}..{}], count={}", begin,
                       end - 1, kvs[begin].first, kvs[end - 1].first,
                       end - begin);
}

}  // namespace

sdk::KVClient make_kv_client(const Options& options) {
    sdk::KVClientConf conf;
    conf.db_name = options.db;
    conf.table_name = options.table;
    conf.sdm_host = options.sdm_host;
    conf.sdm_port = options.sdm_port;
    conf.sdm_timeout_ms = 3000;
    conf.storage_timeout_ms = 3000;
    conf.route_cache_ttl_ms = 0;
    conf.log.level = sdk::LogLevel::INFO;
    conf.log.callback = [](sdk::LogLevel level, std::string_view message) {
        switch (level) {
            case sdk::LogLevel::DEBUG:
                LOG_DEBUG("[adviskv_sdk] {}", message);
                break;
            case sdk::LogLevel::INFO:
                LOG_INFO("[adviskv_sdk] {}", message);
                break;
            case sdk::LogLevel::WARN:
                LOG_WARN("[adviskv_sdk] {}", message);
                break;
            case sdk::LogLevel::ERROR:
                LOG_ERROR("[adviskv_sdk] {}", message);
                break;
        }
    };
    return sdk::KVClient(conf);
}

bool wait_get_value(sdk::KVClient* client, const Options& options,
                    const std::string& key, const std::string& expected) {
    std::string last_error;
    return eventually(
        fmt::format("sdk get {}", key), options,
        [&]() {
            Value value;
            const Status status = client->get(key, &value);
            if (status.fail()) {
                return CheckResult::fail(status.to_string());
            }
            if (value != expected) {
                return CheckResult::fail(fmt::format(
                    "unexpected value '{}', expected '{}'", value, expected));
            }
            return CheckResult::pass(fmt::format("value={}", value));
        },
        &last_error);
}

bool wait_key_not_found(sdk::KVClient* client, const Options& options,
                        const std::string& key) {
    std::string last_error;
    return eventually(
        fmt::format("sdk get deleted {}", key), options,
        [&]() {
            Value value;
            const Status status = client->get(key, &value);
            if (status.code() == StatusCode::KEY_NOT_FOUND) {
                return CheckResult::pass(status.to_string());
            }
            return CheckResult::fail(status.to_string());
        },
        &last_error);
}

std::vector<KV> make_case_kvs(const std::string& prefix, int32_t key_count) {
    std::vector<KV> kvs;
    kvs.reserve(key_count);
    for (int32_t i = 0; i < key_count; ++i) {
        Key key = fmt::format("{}-key-{:03d}", prefix, i);
        Value value = fmt::format("{}-value-{:03d}", prefix, i);
        kvs.push_back(std::make_pair(key, value));
    }
    return kvs;
}

std::vector<Key> make_case_keys(const std::string& prefix, int32_t key_count) {
    std::vector<Key> keys;
    keys.reserve(key_count);
    for (const KV& kv : make_case_kvs(prefix, key_count)) {
        keys.push_back(kv.first);
    }
    return keys;
}

bool write_kvs(sdk::KVClient* client, const Options& options,
               const std::string& name, const std::vector<KV>& kvs) {
    if (static_cast<int32_t>(kvs.size()) <= kVerboseDatasetLogLimit) {
        for (const KV& kv : kvs) {
            const Key key = kv.first;
            const Value value = kv.second;
            if (!wait_status(fmt::format("sdk put {}", key), options,
                             [&]() { return client->put(key, value); })) {
                return false;
            }
        }
        return true;
    }

    for (size_t begin = 0; begin < kvs.size(); begin += kDatasetLogRangeSize) {
        const size_t end = std::min(
            begin + static_cast<size_t>(kDatasetLogRangeSize), kvs.size());
        for (size_t i = begin; i < end; ++i) {
            const Key key = kvs[i].first;
            const Value value = kvs[i].second;
            if (!wait_status_quiet(fmt::format("sdk put {}", key), options,
                                   [&]() { return client->put(key, value); })) {
                return false;
            }
        }
        print_pass(fmt::format("sdk put {}", name),
                   kv_range_message(kvs, begin, end));
    }
    return true;
}

bool verify_kvs(sdk::KVClient* client, const Options& options,
                const std::string& name, const std::vector<KV>& kvs) {
    if (static_cast<int32_t>(kvs.size()) <= kVerboseDatasetLogLimit) {
        for (const KV& kv : kvs) {
            if (!wait_get_value(client, options, kv.first, kv.second)) {
                return false;
            }
        }
        return true;
    }

    for (size_t begin = 0; begin < kvs.size(); begin += kDatasetLogRangeSize) {
        const size_t end = std::min(
            begin + static_cast<size_t>(kDatasetLogRangeSize), kvs.size());
        for (size_t i = begin; i < end; ++i) {
            if (!wait_get_value_quiet(client, options, kvs[i].first,
                                      kvs[i].second)) {
                return false;
            }
        }
        print_pass(fmt::format("sdk get {}", name),
                   kv_range_message(kvs, begin, end));
    }
    return true;
}

bool validate_key_count(const Options& options) {
    if (options.key_count <= 0) {
        print_fail("validate options", "--key_count must be positive");
        return false;
    }
    return true;
}

bool write_dataset(sdk::KVClient* client, const Options& options,
                   const std::string& prefix) {
    return write_kvs(client, options, prefix,
                     make_case_kvs(prefix, options.key_count));
}

bool verify_dataset(sdk::KVClient* client, const Options& options,
                    const std::string& prefix) {
    return verify_kvs(client, options, prefix,
                      make_case_kvs(prefix, options.key_count));
}

}  // namespace adviskv::e2e
