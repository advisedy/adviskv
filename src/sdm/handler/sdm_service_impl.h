#pragma once
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <memory>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm.grpc.pb.h"
#include "sdm/service/heartbeat_service.h"
#include "sdm/service/node_service.h"
#include "sdm/service/route_service.h"
#include "sdm/service/table_service.h"
namespace adviskv::sdm {

class SdmServiceImpl final : public rpc::ShardingManagerService::Service {
   public:
    explicit SdmServiceImpl(TableService* table_service,
                            NodeService* node_service,
                            HeartBeatService* heartbeat_service,
                            RouteService* route_service);

#define DEFINE_METHOD(method_name)                                 \
    grpc::Status method_name(grpc::ServerContext* context,         \
                             const rpc::method_name##Request* req, \
                             rpc::method_name##Response* response) override;

    DEFINE_METHOD(PlaceTable)
    DEFINE_METHOD(DropTable)
    DEFINE_METHOD(GetTableStatus)
    DEFINE_METHOD(PlaceDB)
    DEFINE_METHOD(HeartBeat)
    DEFINE_METHOD(RegisterNode)
    DEFINE_METHOD(GetRoute)

#undef DEFINE_METHOD

   private:
    TableService* table_service_{nullptr};
    NodeService* node_service_{nullptr};
    HeartBeatService* heartbeat_service_{nullptr};
    RouteService* route_service_{nullptr};
};

}  // namespace adviskv::sdm