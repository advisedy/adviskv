#pragma once 


#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/meta_cache_manager.h"
#include "sdm/manager/node_manager.h"
#include "sdm/manager/route_manager.h"
#include <cstdint>
#include <memory>
#include <string>

namespace adviskv{

struct GetRouteParam{
    std::string db_name;
    std::string table_name;
    Key key;

    Status validate()const{
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        return Status::OK();
    }
};


class RouteService{
    
public:

    explicit RouteService(RouteManager* route_manager);
    Status get_route(const GetRouteParam& param, ShardRoute* res) const;
private:
    
    int32_t calc_shard_id(Key key, int32_t shard_count) const;


    RouteManager* route_manager_;
    MetaCacheManager* meta_cache_manager_;
};

}