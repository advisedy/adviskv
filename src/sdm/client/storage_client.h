#pragma once

#include <grpcpp/channel.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
#include "storage.grpc.pb.h"

namespace adviskv::sdm {

struct StorageReplicaInfo {
    ReplicaID replica_id;
    ReplicaRole raft_role{ReplicaRole::FOLLOWER};
    ReplicaPhase status{
        ReplicaPhase::CREATING};  // 代表对应的storage侧的replica
                                  // status转化到sdm的phase, 这里已经转化了
    Endpoint endpoint;
    Term term;
};

class IStorageClient {
   public:
    virtual ~IStorageClient() = default;

    virtual Status create_replica(const CreateReplicaParam& param) = 0;
    virtual Status delete_replica(const DeleteReplicaParam& param) = 0;
    virtual Status get_replica_info(const GetReplicaInfoParam& param,
                                    StorageReplicaInfo& out, bool& exists) = 0;
};

class StorageClient : public IStorageClient {
   public:
    StorageClient() = default;

    Status create_replica(const CreateReplicaParam& param) override;
    Status delete_replica(const DeleteReplicaParam& param) override;
    Status get_replica_info(const GetReplicaInfoParam& param,
                            StorageReplicaInfo& out, bool& exists) override;

   private:
    rpc::StorageService::Stub* make_stub(const std::string& ip, int32_t port);

    // stub_cache_ 的key我们是用IP和port结合在一起
    std::unordered_map<std::string, std::unique_ptr<rpc::StorageService::Stub>>
        stub_cache_;
};
}  // namespace adviskv::sdm