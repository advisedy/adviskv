#pragma once

#include "common/status.h"
#include "storage/model/param.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

// 负责把已经 committed 的 Raft log apply 到状态机。
class ReplicaApplier {
   public:
    ReplicaApplier(ReplicaContext& context);

    // 调用方必须已经持有 state_machine_mutex_，这里不负责吃这个锁。
    Status apply_committed_entries();

   private:
    Status apply_log_entry(const LogEntry& entry);
    Status apply_kv_log_entry(const LogEntry& entry);
    Status apply_config_log_entry(const LogEntry& entry);

    ReplicaContext& context_;
};

}  // namespace adviskv::storage