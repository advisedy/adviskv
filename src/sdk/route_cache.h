#pragma once

#include <memory>

#include "common/define.h"
#include "common/status.h"
#include "sdk/config.h"
#include "sdk/model.h"

namespace adviskv::sdk {

class TopoRouteClient;

class RouteCache {
public:
    RouteCache(const KVClientConf& conf, const TopoRouteClient* route_client);
    ~RouteCache();

    DISALLOW_COPY_AND_ASSIGN(RouteCache)

    Status init();
    void stop();

    Status resolve_route(const Key& key, RouteInfo* route);
    Status refresh();

private:
    struct TableRouteSnapshot;
    class RefreshTask;

    std::shared_ptr<TableRouteSnapshot> load_snapshot() const;
    void store_snapshot(std::shared_ptr<TableRouteSnapshot> snapshot);

    Status lookup_cached_route(const Key& key, RouteInfo* route) const;
    Status publish_table_routes(const TableRouteInfo& table_routes);

    KVClientConf conf_;
    const TopoRouteClient* route_client_{nullptr};
    std::shared_ptr<TableRouteSnapshot> snapshot_;
    std::unique_ptr<RefreshTask> refresh_task_;
};

}  // namespace adviskv::sdk
