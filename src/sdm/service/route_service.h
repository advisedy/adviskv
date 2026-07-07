#pragma once

#include "common/status.h"
#include "common/model/type.h"
#include "sdm/model/param.h"
#include "sdm/model/model.h"

namespace adviskv::sdm {

class SdmStore;

class RouteService {
   public:
    explicit RouteService(SdmStore* store);

    Status get_route(const GetRouteParam& param, ShardRoute* out) const;
    Status get_shard_route(const GetShardRouteParam& param,
                           ShardRoute* out) const;
    Status reconcile_all();

   protected:
    ShardID calc_shard_id(const Table& table, Key key) const;
    static Status validate_writable_route(const ShardRoute& route);
    Status check_shard_route(const Table& table, ShardIndex shard_index);

    SdmStore* store_{nullptr};
};

}  // namespace adviskv::sdm