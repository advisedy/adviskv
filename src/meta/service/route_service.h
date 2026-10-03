#pragma once

#include <vector>

#include "common/model/model.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/reconcile/route_reconciler.h"

namespace adviskv::meta {

class MetaStore;

class RouteService {
public:
    explicit RouteService(MetaStore* store);

    Status reconcile_all();
    Status get_table_routes(const GetTableRoutesParam& param, Table* out_table,
                            std::vector<ShardRoute>* out_routes) const;

protected:
    static Status validate_writable_route(const ShardRoute& route);

    MetaStore* store_{nullptr};
    RouteReconciler reconciler_;
};

}  // namespace adviskv::meta
