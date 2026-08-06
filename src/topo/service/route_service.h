#pragma once

#include <vector>

#include "common/model/type.h"
#include "common/status.h"
#include "topo/model/model.h"
#include "topo/model/param.h"

namespace adviskv::topo {

class TopoStore;

class RouteService {
public:
    explicit RouteService(TopoStore* store);

    Status get_route(const GetRouteParam& param, ShardRoute* out) const;
    Status get_table_routes(const GetTableRoutesParam& param, Table* out_table,
                            std::vector<ShardRoute>* out_routes) const;

    Status reconcile_all();

protected:
    ShardID calc_shard_id(const Table& table, Key key) const;
    static Status validate_writable_route(const ShardRoute& route);
    Status check_shard_route(const Table& table, ShardIndex shard_index);

    TopoStore* store_{nullptr};
};

}  // namespace adviskv::topo