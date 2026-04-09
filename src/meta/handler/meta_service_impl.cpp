
#include "meta/handler/meta_service_impl.h"
#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/service/ddl_service.h"
#include <grpc/grpc.h>
#include <grpcpp/support/status.h>
#include <memory>

namespace adviskv{


MetaServiceImpl::MetaServiceImpl(){
    catalog_manager_ = std::make_unique<CatalogManager>();
    ddl_service_ = std::make_unique<DdlSerivce>(catalog_manager_.get());
}


grpc::Status MetaServiceImpl::CreateDB(grpc::ServerContext* context,
                        const rpc::CreateDBRequest* request,
                        rpc::CreateDBResponse* response) {

    CreateDBParam param{request->db_name()};

    DBMeta db_meta;
    Status status = ddl_service_->create_db(param, &db_meta);
    fill_base_rsp(response, status);
    if(status.fail()){
        return grpc::Status::OK;
    }
    response->set_db_id(db_meta.db_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::CreateTable(grpc::ServerContext* context,
                        const rpc::CreateTableRequest* request,
                        rpc::CreateTableResponse* response) {
    CreateTableParam param{
        .db_name = request->db_name(),
        .table_name = request->table_name(),
        .shard_count = request->shard_count(),
        .replica_count = request->replica_count()
    };
    TableMeta table_meta;
    Status status = ddl_service_->create_table(param, &table_meta);
    fill_base_rsp(response, status);
    if(status.fail()){
        return grpc::Status::OK;
    }
    response->set_table_id(table_meta.table_id);
    return grpc::Status::OK;

}


grpc::Status MetaServiceImpl::GetTable(grpc::ServerContext* context,
    const rpc::GetTableRequest* request,
    rpc::GetTableResponse* response) {

        GetTableParam param{
            .db_name = request->db_name(),
            .table_name = request->table_name()
        };

        TableMeta table_meta;
        Status status = ddl_service_->get_table(param, &table_meta);
        fill_base_rsp(response, status);
        if(status.fail()){
            return grpc::Status::OK;
        }
        response->set_db_id(table_meta.db_id);
        response->set_table_id(table_meta.table_id);
        response->set_replica_count(table_meta.replica_count);
        response->set_shard_count(table_meta.shard_count);
        return grpc::Status::OK;
    }

}