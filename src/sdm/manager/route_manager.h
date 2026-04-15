// #pragma once

// #include "common/status.h"
// #include "common/type.h"
// #include <cstdint>
// #include <functional>
// #include <shared_mutex>
// #include <string>
// #include <unordered_map>
// #include <vector>

// #include "sdm/model/route_model.h"
// namespace adviskv{



// class RouteManager{

// public:  
//     Status update_route(const ShardRoute& route);
//     Status get_route(TableID table_id, ShardID shard_id, ShardRoute* out) const;

// private:
//     mutable std::shared_mutex routes_mutex_;
//     std::unordered_map<ShardRouteKey, ShardRoute, ShardRouteKeyHash> routes_;

// };

// }
