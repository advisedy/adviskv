#pragma once

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "common/proto/codec.h"
#include "meta.grpc.pb.h"

namespace adviskv::meta {

class ServiceManager;

class MetaServiceImpl final : public meta_rpc::MetaService::Service {
public:
    explicit MetaServiceImpl(ServiceManager* service_manager);

    grpc::Status CreateDB(grpc::ServerContext* context, const meta_rpc::CreateDBRequest* request,
                          meta_rpc::CreateDBResponse* response) override;
    grpc::Status DropDB(grpc::ServerContext* context, const meta_rpc::DropDBRequest* request,
                        meta_rpc::DropDBResponse* response) override;
    grpc::Status CreateTable(grpc::ServerContext* context, const meta_rpc::CreateTableRequest* request,
                             meta_rpc::CreateTableResponse* response) override;
    grpc::Status DropTable(grpc::ServerContext* context, const meta_rpc::DropTableRequest* request,
                           meta_rpc::DropTableResponse* response) override;
    grpc::Status AlterTableReplicaCount(grpc::ServerContext* context,
                                        const meta_rpc::AlterTableReplicaCountRequest* request,
                                        meta_rpc::AlterTableReplicaCountResponse* response) override;
    grpc::Status GetTable(grpc::ServerContext* context, const meta_rpc::GetTableRequest* request,
                          meta_rpc::GetTableResponse* response) override;
    grpc::Status GetTableRoutes(grpc::ServerContext* context, const meta_rpc::GetTableRoutesRequest* request,
                                meta_rpc::GetTableRoutesResponse* response) override;
    grpc::Status RegisterNode(grpc::ServerContext* context, const meta_rpc::RegisterNodeRequest* request,
                              meta_rpc::RegisterNodeResponse* response) override;
    grpc::Status Heartbeat(grpc::ServerContext* context, const meta_rpc::HeartbeatRequest* request,
                           meta_rpc::HeartbeatResponse* response) override;

private:
    ServiceManager* service_manager_{nullptr};
};

}  // namespace adviskv::meta
