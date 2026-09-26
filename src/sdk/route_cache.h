#pragma once

#include <memory>

#include "common/define.h"
#include "common/status.h"
#include "sdk/config.h"
#include "sdk/model.h"

namespace adviskv::sdk {

class MetaRouteClient;

class RouteCache {
public:
    RouteCache(const KVClientConf& conf, const MetaRouteClient* route_client);
    ~RouteCache();

    DISALLOW_COPY_AND_ASSIGN(RouteCache)

    Status init();
    void stop();

    Status resolve_route(const Key& key, RouteInfo* route);
    Status refresh(Milliseconds timeout = Milliseconds::zero());

private:
    struct TableRouteSnapshot;
    class RefreshTask;

    std::shared_ptr<TableRouteSnapshot> load_snapshot() const;
    void store_snapshot(std::shared_ptr<TableRouteSnapshot> snapshot);

    Status lookup_cached_route(const Key& key, RouteInfo* route) const;
    Status publish_table_routes(TableRouteInfo table_routes);

    KVClientConf conf_;
    const MetaRouteClient* route_client_{nullptr};
    std::shared_ptr<TableRouteSnapshot> snapshot_;
    std::unique_ptr<RefreshTask> refresh_task_;
};

}  // namespace adviskv::sdk
