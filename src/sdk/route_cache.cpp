#include "sdk/route_cache.h"

#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/background_task.h"
#include "common/define.h"
#include "common/metrics/metrics.h"
#include "common/stable_hash.h"
#include "sdk/log.h"
#include "sdk/sdm_route_client.h"

namespace adviskv::sdk {

struct RouteCache::TableRouteSnapshot {
    TableID table_id{-1};
    int32_t shard_count{0};
    std::vector<RouteInfo> routes;
    // TODO 以后弄version，以免总是这样一直返回整个table的routes
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
        IGNORE_RESULT(cache_->refresh_table_routes())
    }

private:
    RouteCache* cache_{nullptr};
};

RouteCache::RouteCache(const KVClientConf& conf, const SdmRouteClient* route_client)
        : conf_(conf), route_client_(route_client) {}

RouteCache::~RouteCache() { stop(); }

void RouteCache::start() {
    if (refresh_task_ == nullptr) {
        refresh_task_ = std::make_unique<RefreshTask>(this);
    }
    refresh_task_->start(Milliseconds(conf_.route_shard_refresh_interval_ms));
    refresh_task_->notify();
}

void RouteCache::stop() {
    if (refresh_task_ != nullptr) {
        refresh_task_->stop();
    }
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

    // TODO 以后不在SDK这边手动做hash了，让SDM那边传递过来ranges，这边直接做映射。
    ShardIndex shard_id = stable_shard_index(key, snapshot->shard_count);
    if (shard_id < 0 || to<size_t>(shard_id) >= snapshot->routes.size()) {
        return Status::ROUTE_NOT_FOUND(fmt::format("route cache shard_id invalid, shard_id={}, shard_count={}",
                                                   shard_id, snapshot->shard_count));
    }

    const RouteInfo& cached = snapshot->routes[to<size_t>(shard_id)];
    if (cached.replicas.empty()) {
        return Status::ROUTE_NOT_FOUND(fmt::format("route cache shard {} is not ready", shard_id));
    }

    *route = cached;
    return Status::OK();
}

Status RouteCache::resolve_route(const Key& key, RouteInfo* route) {
    ADVISKV_METRICS_TIMER("sdk_route_cache_lookup");
    Status status = lookup_cached_route(key, route);
    if (status.ok()) {
        ADVISKV_METRICS_COUNTER("sdk_route_cache_hit");
        return status;
    }
    // 这里我们对查询路由缓存进行重试，和外部的put的重试并不是一回事
    ADVISKV_METRICS_COUNTER("sdk_route_cache_miss");
    return refresh_route_for_key(key, route);
}

Status RouteCache::refresh_route_for_key(const Key& key, RouteInfo* route) {
    RETURN_IF_NULLPTR(route, "route should not be nullptr")
    ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_foreground");

    RETURN_IF_INVALID_STATUS(refresh_table_routes())
    return lookup_cached_route(key, route);
}

Status RouteCache::refresh_table_routes() {
    ADVISKV_METRICS_TIMER("sdk_route_cache_refresh_table_routes");
    ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_table_routes_request");
    RETURN_IF_NULLPTR(route_client_, "route client is nullptr")

    TableRouteInfo table_routes;
    Status status = route_client_->get_table_routes(&table_routes);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_table_routes_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN, "refresh table routes failed, db={}, table={}, status={}", conf_.db_name,
                        conf_.table_name, status.to_string());
        return status;
    }

    ADVISKV_METRICS_COUNTER("sdk_route_cache_refresh_table_routes_success");
    return publish_table_routes(table_routes);
}

Status RouteCache::publish_table_routes(const TableRouteInfo& table_routes) {
    RETURN_IF_INVALID_CONDITION(table_routes.table_id >= 0, "table_id invalid")
    RETURN_IF_INVALID_CONDITION(table_routes.shard_count > 0, "shard_count invalid")
    RETURN_IF_INVALID_CONDITION(to<int32_t>(table_routes.routes.size()) == table_routes.shard_count,
                                fmt::format("table routes size mismatch, routes={}, shard_count={}",
                                            table_routes.routes.size(), table_routes.shard_count))

    auto next = std::make_shared<TableRouteSnapshot>();
    next->table_id = table_routes.table_id;
    next->shard_count = table_routes.shard_count;
    next->routes = table_routes.routes;
    store_snapshot(next);
    ADVISKV_METRICS_COUNTER("sdk_route_cache_table_routes_update");
    return Status::OK();
}

}  // namespace adviskv::sdk