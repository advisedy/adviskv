#pragma once

#include <cstdint>
#include <vector>

#include "common/define.h"
#include "common/model/engine_type.h"
#include "common/model/ids.h"
#include "common/model/peer_member.h"
#include "common/status.h"
#include "common/types.h"

namespace adviskv {

enum class ExpectedReplicaType : int8 {
    PRESENT = 1,
    ABSENT = 2,
    ADD_MEMBER = 3,
    REMOVE_MEMBER = 4,
};

struct ExpectedReplica {
    ReplicaID replica_id;
    ExpectedReplicaType type{
        ExpectedReplicaType::
            PRESENT};  // 如果是ABSENT的话，不需要填写下面两个变量
    EngineType engine_type{EngineType::MAP};
    std::vector<PeerMember> initial_members;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(
            replica_id.table_id >= 0,
            "replica table id should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(
            replica_id.shard_index >= 0,
            "replica shard index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(
            replica_id.replica_seq >= 0,
            "replica index should be greater than or equal to 0")
        return Status::OK();
    }
};

}  // namespace adviskv