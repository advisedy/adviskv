#pragma once

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

class ReplicaRaftLoop;

// 负责 ReadIndex 读之前的 leader 身份确认。
class ReplicaReadIndexChecker {
   public:
    ReplicaReadIndexChecker(ReplicaContext& context,
                            ReplicaRaftLoop& raft_loop);

    Status check_self_leader_and_get_read_index(LogIndex& read_index);

   private:
    ReplicaContext& context_;
    ReplicaRaftLoop& raft_loop_;
};

}  // namespace adviskv::storage