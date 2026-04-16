// #include "sdm/manager/route_manager.h"
// #include "common/status.h"
// #include <fmt/format.h>
// #include <mutex>

// namespace adviskv{

// Status RouteManager::update_route(const ShardRoute& route){
//     std::unique_lock lock{routes_mutex_};
//     ShardRouteKey key{route.table_id, route.shard_id};
//     auto it = routes_.find(key);
//     if(it == routes_.end()){
//         routes_.insert({key, route});
//     } else{
//         it->second = route;
//     }
//     return Status::OK();
// }

// Status RouteManager::get_route(TableID table_id, ShardID shard_id,
// ShardRoute* out) const{
//     std::shared_lock lock{routes_mutex_};
//     ShardRouteKey key{table_id, shard_id};
//     auto it = routes_.find(key);
//     if(it == routes_.end()){
//         return Status{StatusCode::ROUTE_NOT_FOUND, fmt::format("table_id: {},
//         shard_id: {} not found route", table_id, shard_id)};
//     } else{
//         *out = it->second;
//     }
//     return Status::OK();
// }

// }