#include "sdm/service/route_service.h"

#include <functional>
#include <memory>

#include "common/define.h"
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
    std::shared_ptr<Table> table;
    Status status =
        sdm_store_->get_table_by_name(param.db_name, param.table_name, table);
    RETURN_IF_INVALID_STATUS(status)

    ShardID shard_id = calc_shard_id(*table, param.key);
    std::shared_ptr<ShardRoute> route;
    status = sdm_store_->get_shard_route(shard_id, route);
    RETURN_IF_INVALID_STATUS(status)
    if (route == nullptr) {
        return Status::ROUTE_NOT_FOUND("route not found");
    }
    if (res) {
        *res = *route;
    }

    return status;
}

ShardID RouteService::calc_shard_id(const Table& table, Key key) const {
    // TODO 将来得搞range
    return ShardID{
        .table_id = table.table_id,
        .shard_index = static_cast<ShardIndex>(std::hash<Key>{}(key) %
                                               table.spec.shard_count),
    };
}

}  // namespace adviskv::sdm
