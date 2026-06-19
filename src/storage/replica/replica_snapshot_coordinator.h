#pragma once

#include "common/status.h"
#include "storage/model/param.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

// 负责 follower 接收 leader 快照，以及本地自然生成快照。
class ReplicaSnapshotCoordinator {
   public:
    ReplicaSnapshotCoordinator(ReplicaContext& context,
                               ReplicaRaftEffectRunner& effect_runner);

    Status handle_install_snapshot(const InstallSnapshotParam& param);
    void try_take_snapshot();

   private:
    Status finish_install_snapshot(const InstallSnapshotParam& param);

    ReplicaContext& context_;
    ReplicaRaftEffectRunner& effect_runner_;
};

}  // namespace adviskv::storage