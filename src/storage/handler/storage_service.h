#pragma once
#include "storage.grpc.pb.h"
#include "storage/replica/replica_manager.h"
#include <memory>

namespace adviskv::storage{

class StorageServiceImpl final : public rpc::StorageService::Service{

public:

    explicit StorageServiceImpl(std::unique_ptr<ReplicaManager> replica_manager)
    : replica_manager_(std::move(replica_manager)) {}

    grpc::Status Put(grpc::ServerContext* context,
                const rpc::PutRequest* request,
                rpc::PutResponse* response) override;

    grpc::Status Get(grpc::ServerContext* context,
                const rpc::GetRequest* request,
                rpc::GetResponse* response) override;

    grpc::Status Delete(grpc::ServerContext* context,
                    const rpc::DeleteRequest* request,
                    rpc::DeleteResponse* response) override;

    grpc::Status CreateReplica(grpc::ServerContext* context,
                        const rpc::CreateReplicaRequest* request,
                        rpc::CreateReplicaResponse* response) override;

    grpc::Status DeleteReplica(grpc::ServerContext* context,
                        const rpc::DeleteReplicaRequest* request,
                        rpc::DeleteReplicaResponse* response) override;

    grpc::Status GetReplicaInfo(grpc::ServerContext* context,
                            const rpc::GetReplicaInfoRequest* request,
                            rpc::GetReplicaInfoResponse* response) override;
private:

    std::unique_ptr<ReplicaManager> replica_manager_;



};
}