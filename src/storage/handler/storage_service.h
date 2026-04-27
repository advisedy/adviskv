#pragma once
#include <memory>

#include "storage.grpc.pb.h"
#include "storage/replica/replica_manager.h"

namespace adviskv::storage {

class StorageServiceImpl final : public rpc::StorageService::Service {
   public:
#define DEFINE_METHOD(method_name)                                 \
    grpc::Status method_name(grpc::ServerContext* context,         \
                             const rpc::method_name##Request* req, \
                             rpc::method_name##Response* response) override;

    explicit StorageServiceImpl(std::unique_ptr<ReplicaManager> replica_manager)
        : replica_manager_(std::move(replica_manager)) {}

    DEFINE_METHOD(Put)
    DEFINE_METHOD(Get)
    DEFINE_METHOD(Delete)
    DEFINE_METHOD(CreateReplica)
    DEFINE_METHOD(DeleteReplica)
    DEFINE_METHOD(GetReplicaInfo)
    DEFINE_METHOD(RequestVote)
    DEFINE_METHOD(AppendEntries)

#undef DEFINE_METHOD
    // grpc::Status Put(grpc::ServerContext* context,
    //             const rpc::PutRequest* request,
    //             rpc::PutResponse* response) override;

    // grpc::Status Get(grpc::ServerContext* context,
    //             const rpc::GetRequest* request,
    //             rpc::GetResponse* response) override;

    // grpc::Status Delete(grpc::ServerContext* context,
    //                 const rpc::DeleteRequest* request,
    //                 rpc::DeleteResponse* response) override;

    // grpc::Status CreateReplica(grpc::ServerContext* context,
    //                     const rpc::CreateReplicaRequest* request,
    //                     rpc::CreateReplicaResponse* response) override;

    // grpc::Status DeleteReplica(grpc::ServerContext* context,
    //                     const rpc::DeleteReplicaRequest* request,
    //                     rpc::DeleteReplicaResponse* response) override;

    // grpc::Status GetReplicaInfo(grpc::ServerContext* context,
    //                         const rpc::GetReplicaInfoRequest* request,
    //                         rpc::GetReplicaInfoResponse* response) override;
   private:
    std::unique_ptr<ReplicaManager> replica_manager_;
};
}  // namespace adviskv::storage