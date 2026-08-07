#pragma once

#include "catalog.grpc.pb.h"
#include "catalog.pb.h"

namespace adviskv::catalog {

class DdlService;
class CatalogStore;

class CatalogServiceImpl final : public rpc::CatalogService::Service {
public:
    CatalogServiceImpl() = default;
    explicit CatalogServiceImpl(DdlService* ddl_service);

#define DEFINE_METHOD(method_name)                                                               \
    grpc::Status method_name(grpc::ServerContext* context, const rpc::method_name##Request* req, \
                             rpc::method_name##Response* response) override;

    DEFINE_METHOD(CreateTable)
    // DEFINE_METHOD(DropTable)
    DEFINE_METHOD(AlterTableReplicaCount)
    DEFINE_METHOD(CreateDB)
    DEFINE_METHOD(DropDB)
    DEFINE_METHOD(GetTable)
    DEFINE_METHOD(DropTable)

#undef DEFINE_METHOD

private:
    DdlService* ddl_service_;
};

}  // namespace adviskv::catalog