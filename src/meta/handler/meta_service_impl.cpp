#include "meta/handler/meta_service_impl.h"

#include <vector>

#include "common/define.h"

#include "common/func.h"
#include "common/log.h"
#include "common/proto/codec.h"
#include "common/status.h"
#include "common/types.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/service/service_manager.h"

namespace adviskv::meta {

MetaServiceImpl::MetaServiceImpl(ServiceManager* service_manager) : service_manager_(service_manager) {}

grpc::Status MetaServiceImpl::CreateDB(grpc::ServerContext* context, const meta_rpc::CreateDBRequest* request,
                                       meta_rpc::CreateDBResponse* response) {
    UNUSED(context);
    CreateDBParam param{request->db_name(), request->zone()};
    Database db;
    Status status = service_manager_->create_db(param, &db);
    codec::status_to_base_rsp(response, status);
    if (status.ok()) response->set_db_id(db.db_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::DropDB(grpc::ServerContext* context, const meta_rpc::DropDBRequest* request,
                                     meta_rpc::DropDBResponse* response) {
    UNUSED(context);
    DropDBParam param{request->db_name()};
    Database db;
    Status status = service_manager_->drop_db(param, &db);
    codec::status_to_base_rsp(response, status);
    if (status.ok()) response->set_db_id(db.db_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::CreateTable(grpc::ServerContext* context, const meta_rpc::CreateTableRequest* request,
                                          meta_rpc::CreateTableResponse* response) {
    UNUSED(context);
    CreateTableParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.shard_count = request->shard_count();
    param.replica_count = request->replica_count();
    param.resource_pool = request->resource_pool();
    if (!is_valid_enum_value<EngineType>(request->engine_type())) {
        codec::status_to_base_rsp(response, Status::INVALID_ARGUMENT("engine_type is not valid"));
        return grpc::Status::OK;
    }
    param.engine_type = static_cast<EngineType>(request->engine_type());
    Table table;
    Status status = service_manager_->create_table(param, &table);
    codec::status_to_base_rsp(response, status);
    if (status.ok()) {
        response->set_table_id(table.table_id);
        response->set_phase(static_cast<int32_t>(table.state.phase));
        response->set_operation_id(table.state.operation_id);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::DropTable(grpc::ServerContext* context, const meta_rpc::DropTableRequest* request,
                                        meta_rpc::DropTableResponse* response) {
    UNUSED(context);
    DropTableByNameParam param{request->db_name(), request->table_name()};
    Table table;
    Status status = service_manager_->drop_table(param, &table);
    codec::status_to_base_rsp(response, status);
    if (status.ok()) {
        response->set_table_id(table.table_id);
        response->set_phase(static_cast<int32_t>(table.state.phase));
        response->set_operation_id(table.state.operation_id);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::AlterTableReplicaCount(grpc::ServerContext* context,
                                                     const meta_rpc::AlterTableReplicaCountRequest* request,
                                                     meta_rpc::AlterTableReplicaCountResponse* response) {
    UNUSED(context);
    AlterTableReplicaCountParam param{request->db_name(), request->table_name(), request->replica_count()};
    Table table;
    Status status = service_manager_->alter_table_replica_count(param, &table);
    codec::status_to_base_rsp(response, status);
    if (status.ok()) {
        response->set_table_id(table.table_id);
        response->set_phase(static_cast<int32_t>(table.state.phase));
        response->set_operation_id(table.state.operation_id);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::GetTable(grpc::ServerContext* context, const meta_rpc::GetTableRequest* request,
                                       meta_rpc::GetTableResponse* response) {
    UNUSED(context);
    GetTableParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.use_table_id = request->use_table_id();
    param.table_id = request->table_id();
    Table table;
    Status status = service_manager_->get_table(param, &table);
    codec::status_to_base_rsp(response, status);
    if (status.fail()) return grpc::Status::OK;
    response->set_db_id(table.spec.db_id);
    response->set_table_id(table.table_id);
    response->set_replica_count(table.spec.target_replica_count);
    response->set_shard_count(table.shard_count());
    response->set_phase(static_cast<int32_t>(table.state.phase));
    response->set_last_error_msg(table.state.last_error_msg);
    response->set_operation_id(table.state.operation_id);
    response->set_engine_type(static_cast<int32_t>(table.spec.engine_type));
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::Heartbeat(grpc::ServerContext* context, const meta_rpc::HeartbeatRequest* request,
                                        meta_rpc::HeartbeatResponse* response) {
    UNUSED(context);
    HeartBeatParam param;
    for (const auto& replica_info_pb : request->replica_info_list()) {
        HeartBeatReplicaInfo one;
        if (!codec::decode(replica_info_pb, one)) {
            codec::status_to_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, "replica info is not valid"});
            return grpc::Status::OK;
        }
        param.replica_list.push_back(std::move(one));
    }
    param.node_id = request->node_id();
    param.ip = request->ip();
    param.port = request->port();
    param.resoure_pool_name = request->resource_pool();
    param.dc = request->dc();

    HeartBeatResult result;
    Status status = service_manager_->heartbeat(param, &result);
    if (status.ok()) {
        for (const ExpectedReplica& expect : result.expects) {
            auto* expect_pb = response->add_expects();
            codec::encode(expect, *expect_pb);
        }
    }

    codec::status_to_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::RegisterNode(grpc::ServerContext* context, const meta_rpc::RegisterNodeRequest* request,
                                           meta_rpc::RegisterNodeResponse* response) {
    UNUSED(context);
    RegisterNodeParam param;
    param.node_id = request->node_id();
    param.ip = request->ip();
    param.port = request->port();
    param.resource_pool = request->resource_pool();
    param.dc = request->dc();
    param.last_heartbeat_ts = func::get_current_ts_ms();
    Status status = service_manager_->register_node(param);
    codec::status_to_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::GetTableRoutes(grpc::ServerContext* context,
                                             const meta_rpc::GetTableRoutesRequest* request,
                                             meta_rpc::GetTableRoutesResponse* response) {
    UNUSED(context);

    GetTableRoutesParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.include_unavailable = request->include_unavailable();

    Table table;
    std::vector<ShardRoute> routes;
    Status status = service_manager_->get_table_routes(param, &table, &routes);
    codec::status_to_base_rsp(response, status);
    if (status.fail()) {
        LOG_WARN("[MetaServiceImpl] GetTableRoutes failed, db={}, table={}, status={}", param.db_name, param.table_name,
                 status.to_string());
        return grpc::Status::OK;
    }

    response->set_table_id(table.table_id);
    response->set_shard_count(table.shard_count());
    for (const ShardRoute& route : routes) {
        codec::encode(route, *response->add_routes());
    }

    LOG_INFO("[MetaServiceImpl] GetTableRoutes ok, db={}, table={}, table_id={}, shard_count={}, routes={}",
             param.db_name, param.table_name, table.table_id, table.shard_count(), routes.size());
    return grpc::Status::OK;
}

}  // namespace adviskv::meta
