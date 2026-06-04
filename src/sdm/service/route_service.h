#pragma once

#include <cstdint>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

class RouteService {
   public:
    explicit RouteService(SdmStore* sdm_store);
    Status get_route(const GetRouteParam& param, ShardRoute* res) const;

   private:
    ShardID calc_shard_id(const Table& table, Key key) const;

    SdmStore* sdm_store_;
};

}  // namespace adviskv::sdm