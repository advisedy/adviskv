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

#define DEFINE_METHOD(method_name)                                 \
    grpc::Status method_name(grpc::ServerContext* context,         \
                             const rpc::method_name##Request* req, \
                             rpc::method_name##Response* response) override;

    DEFINE_METHOD(CreateTable)
    // DEFINE_METHOD(DropTable)
    DEFINE_METHOD(AlterTableReplicaCount)
    DEFINE_METHOD(CreateDB)
    DEFINE_METHOD(DropDB)
    DEFINE_METHOD(GetTable)

    grpc::Status DropTable(grpc::ServerContext* context,
                           const rpc::MetaDropTableRequest* request,
                           rpc::MetaDropTableResponse* response) override;

#undef DEFINE_METHOD

   private:
    DdlService* ddl_service_;
};

}  // namespace adviskv::meta