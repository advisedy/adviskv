#pragma once 



#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/meta_cache_manager.h"

#include "sdm/manager/route_manager.h"
#include <cstdint>
#include "sdm/model/service_param.h"
namespace adviskv{



class RouteService{
    
public:

    explicit RouteService(RouteManager* route_manager, MetaCacheManager* meta_cache_manager);
    Status get_route(const GetRouteParam& param, ShardRoute* res) const;
private:
    
    int32_t calc_shard_id(Key key, int32_t shard_count) const;


    RouteManager* route_manager_;
    MetaCacheManager* meta_cache_manager_;
};

}