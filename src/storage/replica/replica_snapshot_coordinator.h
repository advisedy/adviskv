#pragma once

#include <optional>

#include "common/status.h"
#include "storage/model/param.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

class ReplicaRaftLoop;

// 负责 follower 接收 leader 快照，以及本地自然生成快照。
class ReplicaSnapshotCoordinator {
   public:
    ReplicaSnapshotCoordinator(ReplicaContext& context,
                               ReplicaRaftLoop& raft_loop);

    Status handle_install_snapshot(const InstallSnapshotParam& param);
    void try_take_snapshot();

   private:
    struct ReceivingSnapshotSession {
        ReplicaID from_replica_id;
        Term term{0};
        LogIndex snapshot_index{0};
        Term snapshot_term{0};
        uint64 next_offset{0};
    };

    Status check_snapshot_receive_session(const InstallSnapshotParam& param);
    void advance_snapshot_receive_session(const InstallSnapshotParam& param);
    Status finish_install_snapshot(const InstallSnapshotParam& param);
    Status publish_ready_snapshot(const InstallSnapshotContext& context);

    ReplicaContext& context_;
    ReplicaRaftLoop& raft_loop_;
    Optional<ReceivingSnapshotSession> receiving_snapshot_;
};

}  // namespace adviskv::storage