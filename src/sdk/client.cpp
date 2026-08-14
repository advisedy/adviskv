#include "sdk/client.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>
#include <thread>

#include <fmt/format.h>

#include "common/defer.h"
#include "common/define.h"
#include "common/metrics/metrics.h"
#include "common/types.h"
#include "sdk/log.h"

namespace adviskv::sdk {
namespace {

std::string generate_request_id() {
    static const uint64_t instance_id = [] {
        std::random_device rd;
        return (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
    }();
    static std::atomic<uint64_t> sequence{0};
    return fmt::format("adviskv-{}-{}", instance_id, sequence.fetch_add(1, std::memory_order_relaxed));
}


struct ErrorAction {
    bool retryable = false;
    bool refresh_route = false;
};

bool is_uncertain_write_error(StatusCode code) {
    return code == StatusCode::RPC_ERROR || code == StatusCode::NOT_YET_COMMIT;
}

ErrorAction classify_error(StatusCode code) {
    switch (code) {
        case StatusCode::NOT_LEADER:
        case StatusCode::REPLICA_NOT_FOUND:
        case StatusCode::ROUTE_NOT_FOUND:
        case StatusCode::IS_RECOVERING:
        case StatusCode::RPC_ERROR:
            return ErrorAction{/*retryable=*/true, /*refresh_route=*/true};
        case StatusCode::NOT_YET_COMMIT:
        case StatusCode::READ_INDEX_NOT_READY:
        case StatusCode::RESOURCE_EXHAUSTED:
            return ErrorAction{/*retryable=*/true, /*refresh_route=*/false};
        default:
            return ErrorAction{};
    }
}

void sleep_before_retry(SteadyClock::time_point deadline, Milliseconds* retry_delay) {
    auto now = SteadyClock::now();
    if (now >= deadline || retry_delay == nullptr) {
        return;
    }
    auto remaining = std::chrono::duration_cast<Milliseconds>(deadline - now);
    auto sleep_for = std::min(*retry_delay, remaining);
    if (sleep_for.count() > 0) {
        std::this_thread::sleep_for(sleep_for);
    }
    *retry_delay *= 2;
}

}  // namespace

Milliseconds KVClient::remaining_rpc_timeout(SteadyClock::time_point deadline) const {
    Milliseconds remaining = std::chrono::duration_cast<Milliseconds>(deadline - SteadyClock::now());
    if (remaining <= Milliseconds::zero()) {
        return Milliseconds::zero();
    }
    return std::min(remaining, Milliseconds(conf_.rpc_timeout_ms));
}

std::string_view KVClient::operation_name(Operation operation) {
    switch (operation) {
        case Operation::PUT:
            return "put";
        case Operation::DELETE:
            return "delete";
        case Operation::GET:
            return "get";
    }
    return "unknown";
}

void KVClient::record_result(Operation operation, const Status& status) {
    ADVISKV_METRICS_COUNTER(fmt::format("sdk_{}_{}", operation_name(operation), status.ok() ? "success" : "failure"));
}

Status KVClient::finalize_status(const RequestPolicy& policy, Status status, bool write_uncertain) {
    if (policy.is_write_request()) {
        // put / del 操作
        if (!write_uncertain) {
            return status;
        }
        ADVISKV_METRICS_COUNTER(fmt::format("sdk_{}_unknown_final", operation_name(policy.operation)));
        return Status::UNKNOWN(
                fmt::format("write outcome could not be confirmed within SDK retry budget; "
                            "retry the same request_id; a subsequent get is only an application-level check; "
                            "last_status={}; request_id={}",
                            status.to_string(), policy.request_id));
    }
    // get 操作
    if (status.code() != StatusCode::READ_INDEX_NOT_READY) {
        return status;
    }
    status.set_code(StatusCode::SDK_TRY_AGAIN);
    status.set_msg(fmt::format("linearizable read is temporarily unavailable: {}; try again", status.msg()));
    return status;
}

template <typename Send, typename Refresh>
Status KVClient::execute_with_retry(const RequestPolicy& policy, SteadyClock::time_point deadline, Send&& send,
                                    Refresh&& refresh) {
    const std::string_view operation = operation_name(policy.operation);
    const int64_t max_storage_attempts = static_cast<int64_t>(conf_.max_retries) + 1;
    const bool is_write = policy.is_write_request();

    int64_t storage_attempts = 0; // 打向storage的次数
    int64_t route_refreshes = 0; // 路由表刷新的次数
    Milliseconds retry_delay{conf_.retry_delay_ms};
    Status status = Status::RESOURCE_EXHAUSTED("exhausted before request was sent");
    bool write_uncertain = false;  // 在重试内容中是否有出现过status映射为UNKNOWN的情况

    while (true) {
        if (storage_attempts >= max_storage_attempts) {
            break;
        }
        Milliseconds rpc_timeout = remaining_rpc_timeout(deadline);
        if (rpc_timeout <= Milliseconds::zero()) {
            break;
        }

        status = send(rpc_timeout);
        ++storage_attempts;
        if (status.ok()) {
            return status;
        }

        if (is_write && is_uncertain_write_error(status.code())) {
            write_uncertain = true;
        }

        const ErrorAction action = classify_error(status.code());
        if (!action.retryable || storage_attempts >= max_storage_attempts || SteadyClock::now() >= deadline) {
            break;
        }

        if (action.refresh_route) {
            if (route_refreshes >= conf_.route_max_retries) {
                break;
            }
            ++route_refreshes;
            if (Status refresh_status = refresh(); refresh_status.fail()) {
                status = refresh_status;
                break;
            }
        }

        sleep_before_retry(deadline, &retry_delay);
    }

    return finalize_status(policy, status, write_uncertain);
}

KVClient::KVClient(const KVClientConf& conf)
        : conf_(conf), topo_route_client_(conf), route_cache_(conf, &topo_route_client_), storage_client_(conf) {
    IGNORE_RESULT(init());
}

Status KVClient::init() {
    RETURN_IF_INVALID_STATUS(conf_.validate())
    return route_cache_.init();
}

Status KVClient::resolve_route(const Key& key, RouteInfo* route, SteadyClock::time_point deadline) {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")

    Status status = route_cache_.resolve_route(key, route);
    if (status.ok() || status.code() != StatusCode::ROUTE_NOT_FOUND) {
        return status;
    }

    // 冷启动时缓存还没有 route，先主动拉取一次
    ADVISKV_METRICS_COUNTER("sdk_route_resolve_refresh_on_miss");
    return refresh_route(key, route, deadline);
}

Status KVClient::refresh_route(const Key& key, RouteInfo* route, SteadyClock::time_point deadline) {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")

    Milliseconds timeout = remaining_rpc_timeout(deadline);
    if (timeout <= Milliseconds::zero()) {
        return Status::RESOURCE_EXHAUSTED("route refresh deadline exhausted");
    }
    Status status = route_cache_.refresh(timeout);
    if (status.fail()) {
        return status;
    }
    return route_cache_.resolve_route(key, route);
}

template <typename Send>
Status KVClient::run_kv_request(Operation operation, const Key& key, Milliseconds total_timeout,
                                const std::string& request_id, Send&& send) {
    std::string_view op_name = operation_name(operation);
    Status status = Status::OK();
    auto result_guard = Defer([&]() { record_result(operation, status); });

    RETURN_IF_INVALID_STATUS(conf_.validate())

    auto deadline = SteadyClock::now() + total_timeout;
    if (remaining_rpc_timeout(deadline) <= Milliseconds::zero()) {
        status = Status::RESOURCE_EXHAUSTED("timeout exhausted before route was resolved");
        return status;
    }

    RouteInfo route;
    {
        ADVISKV_METRICS_TIMER(fmt::format("sdk_{}_route_resolve", op_name));
        status = resolve_route(key, &route, deadline);
    }
    if (status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN, "{} resolve route failed, db={}, table={}, key={}, status={}", op_name,
                        conf_.db_name, conf_.table_name, key, status.to_string());
        return status;
    }

    RequestPolicy policy{operation, request_id};
    status = execute_with_retry(
            policy, deadline,
            [&](Milliseconds timeout) {
                ADVISKV_METRICS_TIMER(fmt::format("sdk_{}_storage", op_name));
                return send(route, timeout);
            },
            [&]() {
                ADVISKV_METRICS_TIMER(fmt::format("sdk_{}_retry_route_resolve", op_name));
                return refresh_route(key, &route, deadline);
            });
    if (status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN, "{} finished with failure, db={}, table={}, key={}, status={}", op_name,
                        conf_.db_name, conf_.table_name, key, status.to_string());
    }
    return status;
}

Status KVClient::put(const Key& key, const Value& value) { return put(key, value, generate_request_id()); }

Status KVClient::put(const Key& key, const Value& value, const std::string& request_id) {
    ADVISKV_METRICS_TIMER("sdk_put");
    ADVISKV_METRICS_COUNTER("sdk_put_request");
    RETURN_IF_INVALID_CONDITION(!request_id.empty(), "request_id should not be empty")

    return run_kv_request(Operation::PUT, key, Milliseconds(conf_.write_timeout_ms), request_id,
                          [&](const RouteInfo& route, Milliseconds timeout) {
                              return storage_client_.put(route, key, value, request_id, timeout);
                          });
}

Status KVClient::del(const Key& key) { return del(key, generate_request_id()); }

Status KVClient::del(const Key& key, const std::string& request_id) {
    ADVISKV_METRICS_TIMER("sdk_delete");
    ADVISKV_METRICS_COUNTER("sdk_delete_request");
    RETURN_IF_INVALID_CONDITION(!request_id.empty(), "request_id should not be empty")

    return run_kv_request(Operation::DELETE, key, Milliseconds(conf_.write_timeout_ms), request_id,
                          [&](const RouteInfo& route, Milliseconds timeout) {
                              return storage_client_.del(route, key, request_id, timeout);
                          });
}

Status KVClient::get(const Key& key, Value* value) {
    ADVISKV_METRICS_TIMER("sdk_get");
    ADVISKV_METRICS_COUNTER("sdk_get_request");

    RETURN_IF_NULLPTR(value, "value should not be nullptr")

    return run_kv_request(Operation::GET, key, Milliseconds(conf_.read_timeout_ms), "",
                          [&](const RouteInfo& route, Milliseconds timeout) {
                              return storage_client_.get(route, key, value, timeout);
                          });
}

}  // namespace adviskv::sdk