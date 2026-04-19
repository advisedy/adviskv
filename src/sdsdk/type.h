#pragma once

#include <cstdint>

#include "common.pb.h"
#include "common/type.h"

namespace adviskv::sdsdk {

struct ReplicaKey {
    TableID table_id{-1};
    ShardID shard_id{-1};
    int32_t replica_index{-1};
};

struct CreateReplicaArgs {
    ReplicaKey key;
    pb::ReplicaRole role{pb::ReplicaRole::FOLLOWER};
    pb::EngineType engine_type{pb::EngineType::ENGINE_MAP};
};

struct CreateReplicaResult {
    std::string ip;
    int32_t port;
};

struct DeleteReplicaArgs{

};

struct ChangeReplicaRoleArgs{

};

struct ChangeReplicaRoleResult{

};


}  // namespace adviskv::sdsdk