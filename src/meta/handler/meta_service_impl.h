#pragma once

#include "meta.grpc.pb.h"
#include "meta.pb.h"

namespace adviskv::meta {

class DdlService;
class CatalogManager;

class MetaServiceImpl final : public rpc::MetaService::Service {
   public:
    MetaServiceImpl() = default;
    explicit MetaServiceImpl(DdlService* ddl_service);

    grpc::Status CreateTable(grpc::ServerContext* context,
                             const rpc::CreateTableRequest* request,
                             rpc::CreateTableResponse* response) override;

    grpc::Status DropTable(grpc::ServerContext* context,
                           const rpc::MetaDropTableRequest* request,
                           rpc::MetaDropTableResponse* response) override;

    grpc::Status CreateDB(grpc::ServerContext* context,
                          const rpc::CreateDBRequest* request,
                          rpc::CreateDBResponse* response) override;

    grpc::Status DropDB(grpc::ServerContext* context,
                        const rpc::DropDBRequest* request,
                        rpc::DropDBResponse* response) override;

    grpc::Status GetTable(grpc::ServerContext* context,
                          const rpc::GetTableRequest* request,
                          rpc::GetTableResponse* response) override;

   private:
    DdlService* ddl_service_;
};

}  // namespace adviskv::meta