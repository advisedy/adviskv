#pragma once

#include "meta.grpc.pb.h"
#include "meta.pb.h"
#include <memory>




namespace adviskv{

class DdlSerivce;
class CatalogManager;

class MetaServiceImpl final : public rpc::MetaService::Service{

public:

    MetaServiceImpl();

    grpc::Status CreateTable(grpc::ServerContext* context,
                        const rpc::CreateTableRequest* request,
                        rpc::CreateTableResponse* response) override;

    grpc::Status CreateDB(grpc::ServerContext* context,
                        const rpc::CreateDBRequest* request,
                        rpc::CreateDBResponse* response) override;

    grpc::Status GetTable(grpc::ServerContext* context,
                        const rpc::GetTableRequest* request,
                        rpc::GetTableResponse* response) override;


private:
        // std::unique_ptr<CatalogManager> catalog_manager_{std::make_unique<CatalogManager>()};
    std::unique_ptr<DdlSerivce> ddl_service_;
    std::unique_ptr<CatalogManager> catalog_manager_;
};

}