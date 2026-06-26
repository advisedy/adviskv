#pragma once

#include "common/status.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include "sdm/service/heartbeat_service.h"
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
    Status get_table_status(const GetTableStatusParam& param,
                            Table* out_table);

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
    HeartBeatService& heartbeat_service() { return heartbeat_service_; }
    RouteService& route_service() { return route_service_; }
    ReplicaGroupService& replica_group_service() {
        return replica_group_service_;
    }

   private:
    TableService table_service_;
    NodeService node_service_;
    HeartBeatService heartbeat_service_;
    RouteService route_service_;
    ReplicaGroupService replica_group_service_;
};

}  // namespace adviskv::sdm