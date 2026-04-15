#pragma once 



#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/meta_cache_manager.h"

#include "sdm/manager/route_manager.h"
#include <cstdint>
#include "sdm/model/sdm_store.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm{



class RouteService{
    
public:

    explicit RouteService(SdmStore* sdm_store);
    Status get_route(const GetRouteParam& param, ShardRoute* res) const;
private:
    
    int32_t calc_shard_id(const Table& table, Key key) const;


    // RouteManager* route_manager_;
    // MetaCacheManager* meta_cache_manager_;
    SdmStore* sdm_store_;
};

}