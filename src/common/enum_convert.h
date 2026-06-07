#pragma once

#include "common.pb.h"
#include "storage/model/replica_status.h"

namespace adviskv {

pb::ReplicaStatus to_pb_replica_status(storage::ReplicaStatus status);
}  // namespace adviskv
