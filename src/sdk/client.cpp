#include "sdk/client.h"

#include "common/define.h"

namespace adviskv::sdk {

KVClient::KVClient(const KVClientConf& conf)
    : conf_(conf), sdm_route_client_(conf), storage_client_(conf) {}

Status KVClient::put(const Key& key, const Value& value) {
    RETURN_IF_INVALID_PARAM(conf_)

    RouteCacheKey cache_key{conf_.db_name, conf_.table_name, key};
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

Status KVClient::del(const Key& key) {
    RETURN_IF_INVALID_PARAM(conf_)

    RouteCacheKey cache_key{conf_.db_name, conf_.table_name, key};
    RouteInfo route;
    Status status = resolve_route(cache_key, &route);
    RETURN_IF_INVALID_STATUS(status)

    status = storage_client_.del(route, key);
    if (!should_invalidate_route(status)) {
        return status;
    }

    status = resolve_route(cache_key, &route);
    RETURN_IF_INVALID_STATUS(status)
    return storage_client_.del(route, key);
}

Status KVClient::get(const Key& key, Value* value) {
    RETURN_IF_INVALID_PARAM(conf_)
    RETURN_IF_NULLPTR(value, "value should not be nullptr")

    RouteCacheKey cache_key{conf_.db_name, conf_.table_name, key};
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

Status KVClient::resolve_route(const RouteCacheKey& cache_key,
                               RouteInfo* route) {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")

    return sdm_route_client_.get_route(cache_key.key, route);
}

}  // namespace adviskv::sdk