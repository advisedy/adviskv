#pragma once
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <memory>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm.grpc.pb.h"
namespace adviskv::sdm {

class ServiceManager;

class SdmServiceImpl final : public rpc::SdmService::Service {
   public:
    explicit SdmServiceImpl(ServiceManager* service_manager);

#define DEFINE_METHOD(method_name)                                 \
    grpc::Status method_name(grpc::ServerContext* context,         \
                             const rpc::method_name##Request* req, \
                             rpc::method_name##Response* response) override;

    DEFINE_METHOD(PlaceTable)
    DEFINE_METHOD(DropTable)
    DEFINE_METHOD(GetTableStatus)
    DEFINE_METHOD(Heartbeat)
    DEFINE_METHOD(RegisterNode)
    DEFINE_METHOD(GetRoute)

#undef DEFINE_METHOD

    grpc::Status AlterTableReplicaCount(
        grpc::ServerContext* context,
        const rpc::AlterTableReplicaCountRequest* request,
        rpc::AlterTableReplicaCountResponse* response) override;

   private:
    ServiceManager* service_manager_{nullptr};
};

}  // namespace adviskv::sdm