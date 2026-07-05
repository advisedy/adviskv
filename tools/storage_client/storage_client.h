#pragma once

#include <grpcpp/channel.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/model/storage_replica_status.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
#include "storage.grpc.pb.h"

namespace adviskv::tools {

/*
在最开始的架构里面，是SDM主动给storage发消息，但是后来改版成了node agent，
又不希望这段代码直接删掉，万一以后可以测试用，所以就先放在这里了。

target_include_directories(adviskv_tools_storage_client PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
)

include的路径就用tools/storage_client/xxx这种吧

*/

using ::adviskv::sdm::CreateReplicaParam;
using ::adviskv::sdm::DeleteReplicaParam;
using ::adviskv::sdm::GetReplicaInfoParam;

struct StorageReplicaInfo {
    ReplicaID replica_id;
    ReplicaRole raft_role{ReplicaRole::FOLLOWER};
    StorageReplicaStatus storage_status{StorageReplicaStatus::INITIALIZING};
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
}  // namespace adviskv::tools