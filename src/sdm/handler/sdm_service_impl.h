#pragma once
#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm.grpc.pb.h"
#include "sdm/service/placement_service.h"
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <memory>
namespace adviskv {

class SdmServiceImpl final : public rpc::ShardingManagerService::Service {

public:
  explicit SdmServiceImpl(PlacementService *placement_service)
      : placement_service_(placement_service) {}

#define DEFINE_METHOD(method_name)                                             \
  grpc::Status method_name(grpc::ServerContext *context,                       \
                           const rpc::method_name##Request *req,               \
                           rpc::method_name##Response *response) override;

  DEFINE_METHOD(PlaceTable)
  DEFINE_METHOD(PlaceDB)

#undef DEFINE_METHOD

private:
  std::unique_ptr<PlacementService> placement_service_;
};

} // namespace adviskv