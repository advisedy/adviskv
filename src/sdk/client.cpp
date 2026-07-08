#include "sdk/client.h"

#include <fmt/format.h>

#include "common/defer.h"
#include "common/define.h"
#include "common/metrics/metrics.h"
#include "sdk/log.h"

namespace adviskv::sdk {
namespace {

void record_put_result(const Status& status) {
    if (status.ok()) {
        ADVISKV_METRICS_COUNTER("sdk_put_success");
        return;
    }
    ADVISKV_METRICS_COUNTER("sdk_put_failure");
}

void record_delete_result(const Status& status) {
    if (status.ok()) {
        ADVISKV_METRICS_COUNTER("sdk_delete_success");
        return;
    }
    ADVISKV_METRICS_COUNTER("sdk_delete_failure");
}

void record_get_result(const Status& status) {
    if (status.ok()) {
        ADVISKV_METRICS_COUNTER("sdk_get_success");
        return;
    }
    ADVISKV_METRICS_COUNTER("sdk_get_failure");
}

void convert_not_yet_commit_to_unknown(Status* status) {
    if (status == nullptr || status->code() != StatusCode::NOT_YET_COMMIT) {
        return;
    }
    status->set_code(StatusCode::UNKNOWN);
    status->set_msg(
            fmt::format("request result is unknown: {}; retry later or confirm the result "
                        "with a subsequent get",
                        status->msg()));
}

}  // namespace

KVClient::KVClient(const KVClientConf& conf)
        : conf_(conf), sdm_route_client_(conf), route_cache_(conf, &sdm_route_client_), storage_client_(conf) {
    route_cache_.start();
}

Status KVClient::put(const Key& key, const Value& value) {
    ADVISKV_METRICS_TIMER("sdk_put");
    ADVISKV_METRICS_COUNTER("sdk_put_request");

    Status status = Status::OK();
    auto put_result_guard = Defer([&status]() { record_put_result(status); });

    status = conf_.validate();
    if (status.fail()) {
        return status;
    }

    RouteInfo route;
    {
        ADVISKV_METRICS_TIMER("sdk_put_route_resolve");
        status = resolve_route(key, &route);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_put_route_resolve_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "put resolve route failed, db={}, table={}, key={}, "
                        "status={}",
                        conf_.db_name, conf_.table_name, key, status.to_string());
        return status;
    }

    {
        ADVISKV_METRICS_TIMER("sdk_put_storage");
        status = storage_client_.put(route, key, value);
    }
    if (!should_invalidate_route(status)) {
        // 说明route是对的
        convert_not_yet_commit_to_unknown(&status);
        return status;
    }

    // 说明route不对了，重新resolve一下
    ADVISKV_METRICS_COUNTER("sdk_put_route_invalidated");
    ADVISKV_SDK_LOG(LogLevel::INFO,
                    "put invalidates route, db={}, table={}, key={}, "
                    "status={}",
                    conf_.db_name, conf_.table_name, key, status.to_string());
    {
        ADVISKV_METRICS_TIMER("sdk_put_retry_route_resolve");
        status = refresh_route(key, &route);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_put_retry_route_resolve_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "put retry resolve route failed, db={}, table={}, "
                        "key={}, status={}",
                        conf_.db_name, conf_.table_name, key, status.to_string());
        return status;
    }
    {
        ADVISKV_METRICS_TIMER("sdk_put_retry_storage");
        status = storage_client_.put(route, key, value);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_put_retry_failure");
        convert_not_yet_commit_to_unknown(&status);
        ADVISKV_SDK_LOG(LogLevel::WARN, "put retry failed, db={}, table={}, key={}, status={}", conf_.db_name,
                        conf_.table_name, key, status.to_string());
    } else {
        ADVISKV_METRICS_COUNTER("sdk_put_retry_success");
    }
    return status;
}

Status KVClient::del(const Key& key) {
    ADVISKV_METRICS_TIMER("sdk_delete");
    ADVISKV_METRICS_COUNTER("sdk_delete_request");

    Status status = Status::OK();
    auto delete_result_guard = Defer([&status]() { record_delete_result(status); });

    status = conf_.validate();
    if (status.fail()) {
        return status;
    }

    RouteInfo route;
    {
        ADVISKV_METRICS_TIMER("sdk_delete_route_resolve");
        status = resolve_route(key, &route);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_delete_route_resolve_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "delete resolve route failed, db={}, table={}, key={}, "
                        "status={}",
                        conf_.db_name, conf_.table_name, key, status.to_string());
        return status;
    }

    {
        ADVISKV_METRICS_TIMER("sdk_delete_storage");
        status = storage_client_.del(route, key);
    }
    if (!should_invalidate_route(status)) {
        convert_not_yet_commit_to_unknown(&status);
        return status;
    }

    ADVISKV_METRICS_COUNTER("sdk_delete_route_invalidated");
    ADVISKV_SDK_LOG(LogLevel::INFO,
                    "delete invalidates route, db={}, table={}, key={}, "
                    "status={}",
                    conf_.db_name, conf_.table_name, key, status.to_string());
    {
        ADVISKV_METRICS_TIMER("sdk_delete_retry_route_resolve");
        status = refresh_route(key, &route);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_delete_retry_route_resolve_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "delete retry resolve route failed, db={}, table={}, "
                        "key={}, status={}",
                        conf_.db_name, conf_.table_name, key, status.to_string());
        return status;
    }
    {
        ADVISKV_METRICS_TIMER("sdk_delete_retry_storage");
        status = storage_client_.del(route, key);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_delete_retry_failure");
        convert_not_yet_commit_to_unknown(&status);
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "delete retry failed, db={}, table={}, key={}, "
                        "status={}",
                        conf_.db_name, conf_.table_name, key, status.to_string());
    } else {
        ADVISKV_METRICS_COUNTER("sdk_delete_retry_success");
    }
    return status;
}

Status KVClient::get(const Key& key, Value* value) {
    ADVISKV_METRICS_TIMER("sdk_get");
    ADVISKV_METRICS_COUNTER("sdk_get_request");

    RETURN_IF_NULLPTR(value, "value should not be nullptr")

    Status status = Status::OK();
    auto get_result_guard = Defer([&status]() { record_get_result(status); });

    status = conf_.validate();
    if (status.fail()) {
        return status;
    }

    RouteInfo route;
    {
        ADVISKV_METRICS_TIMER("sdk_get_route_resolve");
        status = resolve_route(key, &route);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_get_route_resolve_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "get resolve route failed, db={}, table={}, key={}, "
                        "status={}",
                        conf_.db_name, conf_.table_name, key, status.to_string());
        return status;
    }

    {
        ADVISKV_METRICS_TIMER("sdk_get_storage");
        status = storage_client_.get(route, key, value);
    }
    if (!should_invalidate_route(status)) {
        convert_not_yet_commit_to_unknown(&status);
        return status;
    }

    ADVISKV_METRICS_COUNTER("sdk_get_route_invalidated");
    ADVISKV_SDK_LOG(LogLevel::INFO, "get re-resolves route, db={}, table={}, key={}, status={}", conf_.db_name,
                    conf_.table_name, key, status.to_string());
    {
        ADVISKV_METRICS_TIMER("sdk_get_retry_route_resolve");
        status = refresh_route(key, &route);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_get_retry_route_resolve_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "get retry resolve route failed, db={}, table={}, "
                        "key={}, status={}",
                        conf_.db_name, conf_.table_name, key, status.to_string());
        return status;
    }
    {
        ADVISKV_METRICS_TIMER("sdk_get_retry_storage");
        status = storage_client_.get(route, key, value);
    }
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_get_retry_failure");
        convert_not_yet_commit_to_unknown(&status);
        ADVISKV_SDK_LOG(LogLevel::WARN, "get retry failed, db={}, table={}, key={}, status={}", conf_.db_name,
                        conf_.table_name, key, status.to_string());
    } else {
        ADVISKV_METRICS_COUNTER("sdk_get_retry_success");
    }
    return status;
}

bool KVClient::should_invalidate_route(const Status& status) {
    return status.code() == StatusCode::NOT_LEADER || status.code() == StatusCode::REPLICA_NOT_FOUND ||
           status.code() == StatusCode::ROUTE_NOT_FOUND;
}

Status KVClient::resolve_route(const Key& key, RouteInfo* route) {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")
    return route_cache_.resolve_route(key, route);
}

Status KVClient::refresh_route(const Key& key, RouteInfo* route) {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")

    return route_cache_.refresh_route_for_key(key, route);
}

}  // namespace adviskv::sdk