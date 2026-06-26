#include "sdm/service/service_manager.h"

#include "sdm/selector/node_selector/node_selector.h"

namespace adviskv::sdm {

ServiceManager::ServiceManager(SdmStore* store, NodeSelector* selector)
    : table_service_(store),
      node_service_(store),
      route_service_(store),
      replica_group_service_(store, selector) {}

Status ServiceManager::place_table(const PlaceTableParam& param) {
    return table_service_.place_table(param);
}

Status ServiceManager::drop_table(const DropTableParam& param) {
    return table_service_.drop_table(param);
}

Status ServiceManager::alter_table_replica_count(
    const AlterReplicaCountParam& param) {
    return table_service_.alter_table_replica_count(param);
}

Status ServiceManager::get_table_status(const GetTableStatusParam& param,
                                        Table* out_table) {
    return table_service_.get_table_status(param, out_table);
}

Status ServiceManager::register_node(const RegisterNodeParam& param) {
    return node_service_.register_node(param);
}

Status ServiceManager::heartbeat(const HeartBeatParam& param,
                                 HeartBeatResult* result) {
    Status status = node_service_.heartbeat(param);
    RETURN_IF_INVALID_STATUS(status)
    if (result == nullptr) {
        return Status::OK();
    }
    return replica_group_service_.build_heartbeat_result(param, *result);
}

Status ServiceManager::get_route(const GetRouteParam& param,
                                 ShardRoute* out) const {
    return route_service_.get_route(param, out);
}

Status ServiceManager::reconcile_tables() {
    return table_service_.reconcile_all();
}

Status ServiceManager::reconcile_nodes() {
    return node_service_.reconcile_all();
}

Status ServiceManager::reconcile_routes() {
    return route_service_.reconcile_all();
}

Status ServiceManager::reconcile_replica_groups() {
    return replica_group_service_.reconcile_all();
}

}  // namespace adviskv::sdm