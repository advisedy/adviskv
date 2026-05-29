#include "storage/raft/raft_background_task.h"

#include "storage/replica/replica.h"
#include "storage/replica/replica_manager.h"

namespace adviskv::storage {

RaftTickTask::RaftTickTask(ReplicaManager* manager)
    : BackgroundTask(), manager_(manager) {}

void RaftTickTask::run() {
    std::vector<ReplicaPtr>&& replicas = manager_->get_replicas();
    for (ReplicaPtr& replica : replicas) {
        if (!replica) continue;
        replica->on_tick();
    }
}

}  // namespace adviskv::storage