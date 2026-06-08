#pragma once

#include "common/model/storage_replica_status.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

ReplicaPhase project_phase_from_storage_status(
    ReplicaDesired desired, StorageReplicaStatus storage_status);

}  // namespace adviskv::sdm
