#pragma once

#include "common.pb.h"
#include "common/type.h"
#include "storage/model/replica_status.h"

namespace adviskv {

bool convert_pb_to_replica_role(pb::ReplicaRole in, ReplicaRole& out);
bool convert_replica_role_to_pb(ReplicaRole in, pb::ReplicaRole& out);

pb::ReplicaRole to_pb_replica_role(ReplicaRole role);
pb::ReplicaStatus to_pb_replica_status(storage::ReplicaStatus status);

}  // namespace adviskv
