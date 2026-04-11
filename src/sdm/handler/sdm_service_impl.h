#pragma once
#include "sdm.grpc.pb.h"
#include "common/status.h"
#include "common/type.h"
#include "common/define.h"
#include "common/log.h"
#include "sdm/service/placement_service.h"
#include <memory>
namespace adviskv {

class SdmServiceImpl final : public rpc::ShardingManagerService::Service {

public:
    explicit SdmServiceImpl(PlacementService* placement_service)
    : placement_service_(placement_service) {}

    grpc::Status PlaceTable(grpc::ServerContext* context,
                            const rpc::PlaceTableRequest* request,
                            rpc::PlaceTableResponse* response) override;
private:
    std::unique_ptr<PlacementService> placement_service_;
};


}