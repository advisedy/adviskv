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

    // 直接调用时仍返回该阶段的 Status；后台 tick 会忽略它，由具体 reconciler 负责记录日志。
    Status reconcile_tables();
    Status reconcile_nodes();
    Status reconcile_routes();
    Status reconcile_replica_groups();

    // 单后台任务入口，按依赖顺序推进一轮收敛：
    //   节点存活 -> 副本组与成员 -> 路由 -> 表操作完成判断
    // tick 只负责定序，具体工作仍在各 service 的 reconcile_* 里
    // （经由下面的 reconcile_tables/nodes/routes/replica_groups 转发）。
    // 各阶段自行记录执行错误；tick 只负责定序，不向上汇总阶段错误。
    // 注意：tick 只改善流程表达，不消除并发窗口——DDL 与 heartbeat
    // 仍可在阶段之间进入，各阶段须重读当前状态、幂等重入。
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
