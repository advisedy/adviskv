#include "storage/raft/raft_background_task.h"

#include "storage/replica/replica.h"
#include "storage/replica/replica_manager.h"

namespace adviskv::storage {

RaftTickTask::RaftTickTask(ReplicaManager* manager)
    : manager_(manager), BackgroundTask() {}

void RaftTickTask::run() {
    std::vector<Replica*>&& replicas = manager_->get_replicas();
    for (Replica* replica : replicas) {
        if (!replica) continue;
        replica->on_tick();
    }
}

}  // namespace adviskv::storage