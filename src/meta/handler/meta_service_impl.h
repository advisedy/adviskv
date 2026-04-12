#pragma once

#include "meta.grpc.pb.h"
#include "meta.pb.h"
#include <memory>




namespace adviskv{

class DdlService;
class CatalogManager;

class MetaServiceImpl final : public rpc::MetaService::Service{

public:

    MetaServiceImpl() = default;
    MetaServiceImpl(DdlService* ddl_service, CatalogManager* catalog_manager);

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
    DdlService* ddl_service_;
    CatalogManager* catalog_manager_;
};

}