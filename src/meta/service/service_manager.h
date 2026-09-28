#pragma once

#include <vector>

#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/service/ddl_service.h"
#include "meta/service/node_service.h"
#include "meta/service/replica_group_service.h"
#include "meta/service/route_service.h"

namespace adviskv::meta {

class NodeSelector;
class MetaStore;

class ServiceManager {
public:
    ServiceManager(MetaStore* store, NodeSelector* selector);

    Status create_db(const CreateDBParam& param, Database* db);
    Status drop_db(const DropDBParam& param, Database* db);
    Status create_table(const CreateTableParam& param, Table* table);
    Status drop_table(const DropTableByNameParam& param, Table* table);
    Status alter_table_replica_count(const AlterTableReplicaCountParam& param, Table* table);
    Status get_table(const GetTableParam& param, Table* table);

    Status get_table_routes(const GetTableRoutesParam& param, Table* out_table,
                            std::vector<ShardRoute>* out_routes) const;

    Status register_node(const RegisterNodeParam& param);
    Status heartbeat(const HeartBeatParam& param, HeartBeatResult* result = nullptr);

    // 阶段错误不汇总；DDL 与心跳可在阶段间并发，收敛器须幂等重入。
    void tick();

    DdlService& ddl_service() { return ddl_service_; }
    NodeService& node_service() { return node_service_; }
    RouteService& route_service() { return route_service_; }
    ReplicaGroupService& replica_group_service() { return replica_group_service_; }

private:
    DdlService ddl_service_;
    NodeService node_service_;
    RouteService route_service_;
    ReplicaGroupService replica_group_service_;
};

}  // namespace adviskv::meta
