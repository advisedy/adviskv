#include "sdk/client.h"

#include "common/define.h"

namespace adviskv::sdk {

KVClient::KVClient(const KVClientConf& conf)
    : conf_(conf),
      sdm_route_client_(conf),
      storage_client_(conf) {}

Status KVClient::put(const std::string& db_name, const std::string& table_name,
                     const Key& key, const Value& value) {
    RETURN_IF_INVALID_PARAM(conf_)
    RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
    RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                "table_name should not empty")

    RouteCacheKey cache_key{db_name, table_name, key};
    RouteInfo route;
    Status status = resolve_route(cache_key, &route);
    RETURN_IF_INVALID_STATUS(status)

    status = storage_client_.put(route, key, value);
    if (!should_invalidate_route(status)) {
        return status;
    }

    status = resolve_route(cache_key, &route);
    RETURN_IF_INVALID_STATUS(status)
    return storage_client_.put(route, key, value);
}

Status KVClient::get(const std::string& db_name, const std::string& table_name,
                     const Key& key, Value* value) {
    RETURN_IF_INVALID_PARAM(conf_)
    RETURN_IF_INVALID_CONDITION(value != nullptr, "value should not be nullptr")
    RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
    RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                "table_name should not empty")

    RouteCacheKey cache_key{db_name, table_name, key};
    RouteInfo route;
    Status status = resolve_route(cache_key, &route);
    RETURN_IF_INVALID_STATUS(status)

    status = storage_client_.get(route, key, value);
    if (!should_invalidate_route(status)) {
        return status;
    }

    status = resolve_route(cache_key, &route);
    RETURN_IF_INVALID_STATUS(status)
    return storage_client_.get(route, key, value);
}

bool KVClient::should_invalidate_route(const Status& status) {
    return status.code() == StatusCode::NOT_LEADER ||
           status.code() == StatusCode::REPLICA_NOT_FOUND ||
           status.code() == StatusCode::ROUTE_NOT_FOUND;
}

Status KVClient::resolve_route(const RouteCacheKey& cache_key, RouteInfo* route) {
    RETURN_IF_INVALID_CONDITION(route != nullptr, "route should not be nullptr")

    return sdm_route_client_.get_route(
        cache_key.db_name, cache_key.table_name, cache_key.key, route);
}

}  // namespace adviskv::sdk
