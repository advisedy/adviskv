#include "meta/service/service_manager.h"

#include "meta/placement/node_selector.h"

namespace adviskv::meta {

ServiceManager::ServiceManager(MetaStore* store, NodeSelector* selector)
        : ddl_service_(store),
          node_service_(store),
          route_service_(store),
          replica_group_service_(store, selector) {}

Status ServiceManager::create_db(const CreateDBParam& param, Database* db) { return ddl_service_.create_db(param, db); }

Status ServiceManager::drop_db(const DropDBParam& param, Database* db) { return ddl_service_.drop_db(param, db); }

Status ServiceManager::create_table(const CreateTableParam& param, Table* table) {
    return ddl_service_.create_table(param, table);
}

Status ServiceManager::drop_table(const DropTableByNameParam& param, Table* table) {
    return ddl_service_.drop_table(param, table);
}

Status ServiceManager::alter_table_replica_count(const AlterTableReplicaCountParam& param, Table* table) {
    return ddl_service_.alter_table_replica_count(param, table);
}

Status ServiceManager::get_table(const GetTableParam& param, Table* table) {
    return ddl_service_.get_table(param, table);
}

Status ServiceManager::get_table_routes(const GetTableRoutesParam& param, Table* out_table,
                                        std::vector<ShardRoute>* out_routes) const {
    return route_service_.get_table_routes(param, out_table, out_routes);
}

Status ServiceManager::register_node(const RegisterNodeParam& param) { return node_service_.register_node(param); }

Status ServiceManager::heartbeat(const HeartBeatParam& param, HeartBeatResult* result) {
    Status status = node_service_.heartbeat(param);
    RETURN_IF_INVALID_STATUS(status)
    if (result == nullptr) {
        return Status::OK();
    }
    return replica_group_service_.build_heartbeat_result(param, *result);
}

void ServiceManager::tick() {
    node_service_.reconcile_all();
    replica_group_service_.reconcile_all();
    route_service_.reconcile_all();
    ddl_service_.reconcile_all();
}

}  // namespace adviskv::meta
