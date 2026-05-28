#pragma once

#include <string>

#include "common/type.h"
#include "e2e_context.h"
#include "sdk/model.h"

namespace adviskv::e2e {

// 获取到route（当然是可用的
bool get_route(E2EContext* context, const Key& key, sdk::RouteInfo* route,
               std::string* error);

// 对于一个route，等待到他的状态是ready
bool wait_route_has_leader(E2EContext* context, const Key& key,
                           sdk::RouteReplica* leader);

// print一下当前key对应的shard的leader的ip和port
bool print_current_leader(E2EContext* context, const std::string& key);

}  // namespace adviskv::e2e