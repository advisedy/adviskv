#include "sdm/service/route_service.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/route_manager.h"
#include <functional>
#include <memory>


namespace adviskv {
    // RouteService::RouteService()explicit RouteService(RouteManager* route_manager);{

RouteService::RouteService(RouteManager* route_manager){
    route_manager_ = route_manager;
}


Status RouteService::get_route(const GetRouteOption& option, ShardRoute* res) const{

    RETURN_IF_INVALID_OPTION(option)

    TableMetaCache table_meta;
    Status status = route_manager_->get_table_meta(option.db_name, option.table_name, &table_meta);

    RETURN_IF_INVALID_STATUS(status)

    TableID table_id = table_meta.table_id;
    ShardID shard_id = calc_shard_id(option.key, table_meta.shard_count);

    status = route_manager_->get_route(table_id, shard_id, res);
    
    RETURN_IF_INVALID_STATUS(status)

    return Status::OK();
}

int32_t RouteService::calc_shard_id(Key key, int32_t shard_count) const {
    return std::hash<Key>{}(key) % shard_count;
}

}