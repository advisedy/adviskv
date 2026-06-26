#pragma once

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

class SdmStore;

class RouteService {
   public:
    explicit RouteService(SdmStore* store);

    Status get_route(const GetRouteParam& param, ShardRoute* out) const;
    Status reconcile_all();

   protected:
    ShardID calc_shard_id(const Table& table, Key key) const;
    Status check_shard_route(const Table& table, ShardIndex shard_index);

    SdmStore* store_{nullptr};
};

}  // namespace adviskv::sdm