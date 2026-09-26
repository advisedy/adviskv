#include "sdk/route_cache.h"

#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/background_task.h"
#include "common/define.h"
#include "common/metrics/metrics.h"
#include "sdk/log.h"
#include "sdk/meta_route_client.h"

namespace adviskv::sdk {

struct RouteCache::TableRouteSnapshot {
    TableID table_id{-1};
    int32_t shard_count{0};
    std::vector<ShardRange> shard_ranges;
    std::vector<RouteInfo> routes;
};

class RouteCache::RefreshTask : public BackgroundTask {
public:
    explicit RefreshTask(RouteCache* cache) : cache_(cache) {}

protected:
    void run() override {
        if (cache_ == nullptr) {
            return;
        }
        ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_background");
        IGNORE_RESULT(cache_->refresh())
    }

private:
    RouteCache* cache_{nullptr};
};

RouteCache::RouteCache(const KVClientConf& conf, const MetaRouteClient* route_client)
        : conf_(conf), route_client_(route_client) {}

RouteCache::~RouteCache() { stop(); }

Status RouteCache::init() {
    RETURN_IF_INVALID_STATUS(conf_.validate())

    if (refresh_task_ == nullptr) {
        refresh_task_ = std::make_unique<RefreshTask>(this);
    }
    refresh_task_->start(Milliseconds(conf_.route_shard_refresh_interval_ms), true);
    return Status::OK();
}

void RouteCache::stop() {
    if (refresh_task_ != nullptr) {
        refresh_task_->stop();
    }
}

Status RouteCache::resolve_route(const Key& key, RouteInfo* route) {
    ADVISKV_METRICS_TIMER("sdk_route_cache_lookup");
    Status status = lookup_cached_route(key, route);
    if (status.ok()) {
        ADVISKV_METRICS_COUNTER("sdk_route_cache_hit");
    } else {
        ADVISKV_METRICS_COUNTER("sdk_route_cache_miss");
    }
    return status;
}

Status RouteCache::refresh(Milliseconds timeout) {
    ADVISKV_METRICS_TIMER("sdk_route_cache_refresh_table_routes");
    ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_table_routes_request");
    RETURN_IF_NULLPTR(route_client_, "route client is nullptr")

    TableRouteInfo table_routes;
    Status status = route_client_->get_table_routes(&table_routes, timeout);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_table_routes_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN, "refresh table routes failed, db={}, table={}, status={}", conf_.db_name,
                        conf_.table_name, status.to_string());
        return status;
    }

    ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_table_routes_success");
    return publish_table_routes(std::move(table_routes));
}

std::shared_ptr<RouteCache::TableRouteSnapshot> RouteCache::load_snapshot() const {
    return std::atomic_load(&snapshot_);
}

void RouteCache::store_snapshot(std::shared_ptr<TableRouteSnapshot> snapshot) {
    std::atomic_store(&snapshot_, std::move(snapshot));
}

Status RouteCache::lookup_cached_route(const Key& key, RouteInfo* route) const {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")

    std::shared_ptr<TableRouteSnapshot> snapshot = load_snapshot();
    if (snapshot == nullptr || snapshot->table_id < 0 || snapshot->shard_count <= 0) {
        return Status::ROUTE_NOT_FOUND("route cache is not ready");
    }

    size_t pos = find_shard_range(snapshot->shard_ranges, hash_bucket_for_key(key));
    if (pos >= snapshot->routes.size()) {
        return Status::ROUTE_NOT_FOUND("key is outside the cached hash ranges");
    }

    const RouteInfo& cached = snapshot->routes[pos];
    if (cached.replicas.empty()) {
        return Status::ROUTE_NOT_FOUND("route cache shard is not ready");
    }

    *route = cached;
    return Status::OK();
}

Status RouteCache::publish_table_routes(TableRouteInfo table_routes) {
    RETURN_IF_INVALID_STATUS(check_table_routes_is_valid(table_routes))

    auto next = std::make_shared<TableRouteSnapshot>();
    next->table_id = table_routes.table_id;
    next->shard_count = table_routes.shard_count;
    next->routes = std::move(table_routes.routes);
    next->shard_ranges.reserve(next->routes.size());
    for (const auto& route : next->routes) {
        next->shard_ranges.push_back({route.shard_id, route.hash_range});
    }
    store_snapshot(std::move(next));
    ADVISKV_METRICS_COUNTER("sdk_route_cache_table_routes_update");
    return Status::OK();
}

}  // namespace adviskv::sdk
