#pragma once

#include "common.pb.h"
#include "common/type.h"

namespace adviskv {

bool convert_pb_to_replica_role(pb::ReplicaRole in, ReplicaRole& out);
bool convert_replica_role_to_pb(ReplicaRole in, pb::ReplicaRole& out);

bool convert_pb_to_replica_status(pb::ReplicaStatus in, ReplicaStatus& out);
bool convert_replica_status_to_pb(ReplicaStatus in, pb::ReplicaStatus& out);

pb::ReplicaRole to_pb_replica_role(ReplicaRole role);
pb::ReplicaStatus to_pb_replica_status(ReplicaStatus status);

}  // namespace adviskv
