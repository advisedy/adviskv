#pragma once

#include <vector>

#include "common/status.h"
#include "sdm/model/param.h"
#include "sdm/model/model.h"
#include "sdm/service/node_service.h"
#include "sdm/service/replica_group_service.h"
#include "sdm/service/route_service.h"
#include "sdm/service/table_service.h"

namespace adviskv::sdm {

class NodeSelector;
class SdmStore;

class ServiceManager {
   public:
    ServiceManager(SdmStore* store, NodeSelector* selector);

    Status place_table(const PlaceTableParam& param);
    Status drop_table(const DropTableParam& param);
    Status alter_table_replica_count(
        const AlterReplicaCountParam& param);
    Status get_table_status(const GetTableStatusParam& param,
                            Table* out_table);
    Status get_table_meta(const GetTableMetaParam& param,
                          Table* out) const;
    Status get_table_routes(const GetTableRoutesParam& param, Table* out_table,
                            std::vector<ShardRoute>* out_routes) const;

    Status register_node(const RegisterNodeParam& param);
    Status heartbeat(const HeartBeatParam& param,
                     HeartBeatResult* result = nullptr);
    Status get_route(const GetRouteParam& param, ShardRoute* out) const;

    Status reconcile_tables();
    Status reconcile_nodes();
    Status reconcile_routes();
    Status reconcile_replica_groups();

    TableService& table_service() { return table_service_; }
    NodeService& node_service() { return node_service_; }
    RouteService& route_service() { return route_service_; }
    ReplicaGroupService& replica_group_service() {
        return replica_group_service_;
    }

   private:
    TableService table_service_;
    NodeService node_service_;
    RouteService route_service_;
    ReplicaGroupService replica_group_service_;
};

}  // namespace adviskv::sdm