#include "e2e_kv_util.h"

#include <fmt/core.h>

#include "e2e_assert.h"
#include "sdk/config.h"
#include "sdk/model.h"

namespace adviskv::e2e {

sdk::KVClient make_kv_client(const Options& options) {
    sdk::KVClientConf conf;
    conf.db_name = options.db;
    conf.table_name = options.table;
    conf.sdm_host = options.sdm_host;
    conf.sdm_port = options.sdm_port;
    conf.sdm_timeout_ms = 3000;
    conf.storage_timeout_ms = 3000;
    conf.route_cache_ttl_ms = 0;
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

}  // namespace adviskv::e2e