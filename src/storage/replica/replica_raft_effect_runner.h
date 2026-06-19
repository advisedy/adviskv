#pragma once

#include <functional>
#include <vector>

#include "common/status.h"
#include "storage/model/param.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

// 负责把 RaftNode 产出的 effects 真正落到持久化和网络 RPC 上。
class ReplicaRaftEffectRunner {
   public:
    using RaftStepFunc = std::function<Status(RaftEffects&)>;

    explicit ReplicaRaftEffectRunner(ReplicaContext& context);

    Status run_raft_step(RaftStepFunc&& step);
    Status persist_raft_effects(const RaftEffects& effects);

   private:
    Status send_raft_messages(std::vector<RaftMessage> messages);
    Status send_raft_message(const RaftMessage& msg);

    ReplicaContext& context_;
};

}  // namespace adviskv::storage