
#include "meta/handler/meta_service_impl.h"

#include <memory>

#include <grpc/grpc.h>
#include <grpcpp/support/status.h>

#include "common/define.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/proto/table_state_proto.h"
#include "meta/service/ddl_service.h"

namespace adviskv::meta {

MetaServiceImpl::MetaServiceImpl(DdlService* ddl_service) : ddl_service_(ddl_service) {}

grpc::Status MetaServiceImpl::CreateDB(grpc::ServerContext* context, const rpc::CreateDBRequest* request,
                                       rpc::CreateDBResponse* response) {
    UNUSED(context);
    CreateDBParam param{request->db_name(), request->zone()};

    DBMeta db_meta;
    Status status = ddl_service_->create_db(param, &db_meta);
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    response->set_db_id(db_meta.db_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::DropDB(grpc::ServerContext* context, const rpc::DropDBRequest* request,
                                     rpc::DropDBResponse* response) {
    UNUSED(context);
    DropDBParam param{request->db_name()};

    DBMeta db_meta;
    Status status = ddl_service_->drop_db(param, &db_meta);
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    response->set_db_id(db_meta.db_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::CreateTable(grpc::ServerContext* context, const rpc::CreateTableRequest* request,
                                          rpc::CreateTableResponse* response) {
    UNUSED(context);
    CreateTableParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.shard_count = request->shard_count();
    param.replica_count = request->replica_count();
    param.resource_pool = request->resource_pool();
    if (!decode_pb_engine_type(request->engine_type(), param.engine_type)) {
        fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, "engine_type is not valid"});
        return grpc::Status::OK;
    }

    TableMeta table_meta;
    Status status = ddl_service_->create_table(param, &table_meta);
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    pb::MetaTableState table_state_pb;
    if (!encode_pb_meta_table_state(table_meta.state, table_state_pb)) {
        fill_base_rsp(response, Status{StatusCode::ERROR, "table_state is not valid"});
        return grpc::Status::OK;
    }
    response->set_table_id(table_meta.table_id);
    response->set_table_state(table_state_pb);
    response->set_operation_id(table_meta.operation_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::DropTable(grpc::ServerContext* context, const rpc::DropTableRequest* request,
                                        rpc::DropTableResponse* response) {
    UNUSED(context);
    DropTableParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();

    TableMeta table_meta;
    Status status = ddl_service_->drop_table(param, &table_meta);
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    pb::MetaTableState table_state_pb;
    if (!encode_pb_meta_table_state(table_meta.state, table_state_pb)) {
        fill_base_rsp(response, Status{StatusCode::ERROR, "table_state is not valid"});
        return grpc::Status::OK;
    }
    response->set_table_id(table_meta.table_id);
    response->set_table_state(table_state_pb);
    response->set_operation_id(table_meta.operation_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::AlterTableReplicaCount(grpc::ServerContext* context,
                                                     const rpc::AlterTableReplicaCountRequest* request,
                                                     rpc::AlterTableReplicaCountResponse* response) {
    UNUSED(context);
    AlterTableReplicaCountParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.replica_count = request->replica_count();

    TableMeta table_meta;
    Status status = ddl_service_->alter_table_replica_count(param, &table_meta);
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    pb::MetaTableState table_state_pb;
    if (!encode_pb_meta_table_state(table_meta.state, table_state_pb)) {
        fill_base_rsp(response, Status{StatusCode::ERROR, "table_state is not valid"});
        return grpc::Status::OK;
    }
    response->set_table_id(table_meta.table_id);
    response->set_table_state(table_state_pb);
    response->set_operation_id(table_meta.operation_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::GetTable(grpc::ServerContext* context, const rpc::GetTableRequest* request,
                                       rpc::GetTableResponse* response) {
    UNUSED(context);

    GetTableParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.use_table_id = request->use_table_id();
    param.table_id = request->table_id();

    TableMeta table_meta;
    Status status = ddl_service_->get_table(param, &table_meta);
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    pb::MetaTableState table_state_pb;
    pb::EngineType engine_type_pb;
    if (!encode_pb_meta_table_state(table_meta.state, table_state_pb)) {
        fill_base_rsp(response, Status{StatusCode::ERROR, "table_state is not valid"});
        return grpc::Status::OK;
    }
    if (!encode_pb_engine_type(table_meta.engine_type, engine_type_pb)) {
        fill_base_rsp(response, Status{StatusCode::ERROR, "engine_type is not valid"});
        return grpc::Status::OK;
    }
    response->set_db_id(table_meta.db_id);
    response->set_table_id(table_meta.table_id);
    response->set_replica_count(table_meta.replica_count);
    response->set_shard_count(table_meta.shard_count);
    response->set_table_state(table_state_pb);
    response->set_last_error_msg(table_meta.last_error_msg);
    response->set_operation_id(table_meta.operation_id);
    response->set_engine_type(engine_type_pb);
    return grpc::Status::OK;
}
}  // namespace adviskv::meta