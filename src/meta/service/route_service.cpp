#include "meta/service/route_service.h"

#include <algorithm>

#include <fmt/format.h>

#include "common/status.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

RouteService::RouteService(MetaStore* store) : store_(store), reconciler_(store) {}

Status RouteService::reconcile_all() { return reconciler_.run(); }

Status RouteService::validate_writable_route(const ShardRoute& route) {
    int leader_count = std::count_if(route.replicas.begin(), route.replicas.end(), [](const RouteEntry& entry) {
        return entry.role == ReplicaRole::LEADER && !entry.ip.empty() && entry.port > 0;
    });
    if (leader_count != 1) {
        return Status::ROUTE_NOT_FOUND(
                fmt::format("writable leader route is not ready。 leader_count={}", leader_count));
    }
    return Status::OK();
}

Status RouteService::get_table_routes(const GetTableRoutesParam& param, Table* out_table,
                                      std::vector<ShardRoute>* out_routes) const {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(out_routes, "out_routes is nullptr")
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    return store_->read_with([&](const MetaStoreTxn& txn) -> Status {
        TableOr table;
        RETURN_IF_INVALID_STATUS(txn.get_table_by_name(param.db_name, param.table_name, table))
        if (table.is_empty()) {
            return Status::TABLE_NOT_FOUND(fmt::format("table {}.{} not found", param.db_name, param.table_name));
        }

        std::vector<ShardRoute> routes;
        RETURN_IF_INVALID_STATUS(check_shard_ranges_is_valid(table->shard_count(), table->spec.shard_ranges))
        routes.reserve(to<size_t>(table->shard_count()));
        for (const ShardRange& range : table->spec.shard_ranges) {
            ShardIndex shard_index = range.shard_id;
            ShardRouteOr route;
            ShardID shard_id{table->table_id, shard_index};
            RETURN_IF_INVALID_STATUS(txn.get_shard_route(shard_id, route))
            if (route.is_empty() && !param.include_unavailable) {
                return Status::ROUTE_NOT_FOUND(
                        fmt::format("route not found, table_id={}, shard_id={}", table->table_id, shard_index));
            }
            ShardRoute entry;
            entry.shard_id = shard_id;
            if (route.has_value()) entry = *route;
            if (!param.include_unavailable) {
                RETURN_IF_INVALID_STATUS(validate_writable_route(entry))
            }
            entry.hash_range = range.range;
            routes.push_back(std::move(entry));
        }

        if (out_table != nullptr) {
            *out_table = *table;
        }
        *out_routes = std::move(routes);
        return Status::OK();
    });
}

}  // namespace adviskv::meta
