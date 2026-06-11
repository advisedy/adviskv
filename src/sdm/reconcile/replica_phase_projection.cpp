#include "sdm/reconcile/replica_phase_projection.h"

namespace adviskv::sdm {

ReplicaPhase project_phase_from_storage_status(
    ReplicaDesired desired, StorageReplicaStatus storage_status) {
    if (desired == ReplicaDesired::ABSENT) {
        return ReplicaPhase::DELETING;
    }

    switch (storage_status) {
        case StorageReplicaStatus::INITIALIZING:
        case StorageReplicaStatus::RECOVERING:
            return ReplicaPhase::CREATING;
        case StorageReplicaStatus::READY:
            return ReplicaPhase::READY;
        case StorageReplicaStatus::FAULTED:
        default:
            return ReplicaPhase::ERROR;
    }
}

}  // namespace adviskv::sdm
