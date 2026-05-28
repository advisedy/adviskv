#include "sdm/service/route_service.h"

#include <fmt/format.h>

#include <algorithm>
#include <functional>
#include <memory>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/meta_cache_manager.h"
#include "sdm/manager/route_manager.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {
// RouteService::RouteService()explicit RouteService(RouteManager*
// route_manager);{

RouteService::RouteService(SdmStore* sdm_store) : sdm_store_(sdm_store) {}

Status RouteService::get_route(const GetRouteParam& param,
                               ShardRoute* res) const {
    RETURN_IF_INVALID_PARAM(param)
    TableOr table;
    Status status =
        sdm_store_->get_table_by_name(param.db_name, param.table_name, table);
    RETURN_IF_INVALID_STATUS(status)
    if (table.is_empty()) {
        return Status::TABLE_NOT_FOUND(fmt::format(
            "table {}.{} not found", param.db_name, param.table_name));
    }

    ShardID shard_id = calc_shard_id(*table, param.key);
    ShardRouteOr route;
    status = sdm_store_->get_shard_route(shard_id, route);
    RETURN_IF_INVALID_STATUS(status)
    if (route.is_empty()) {
        return Status::ROUTE_NOT_FOUND("route not found");
    }
    int leader_count =
        std::count_if(route->replicas.begin(), route->replicas.end(),
                      [](const RouteEntry& entry) {
                          return entry.role == ReplicaRole::LEADER &&
                                 !entry.ip.empty() && entry.port > 0;
                      });
    if (leader_count != 1) {
        return Status::ROUTE_NOT_FOUND(
            fmt::format("writable leader route is not ready。 leader_count={}",
                        leader_count));
    }
    if (res) {
        *res = *route;
        // 这边第一个就是leader的，
        // 这个是在route_updater那边就保证了的。
    }

    {  // 打一下日志
        std::string route_res{route->shard_id.to_string()};
        for (RouteEntry& one : route->replicas) {
            route_res.append(" replica: " + one.replica_id.to_string() + ", ");
            if (one.role == ReplicaRole::LEADER) {
                route_res.append("role: leader.");
            } else {
                route_res.append("role: follower.");
            }
        }
        LOG_DEBUG("route is ok, {}", route_res);
    }

    return status;
}

ShardID RouteService::calc_shard_id(const Table& table, Key key) const {
    // TODO 将来得搞range
    return ShardID{
        table.table_id, static_cast<ShardIndex>(std::hash<Key>{}(key) %
                                                table.spec.shard_count)};
}

}  // namespace adviskv::sdm