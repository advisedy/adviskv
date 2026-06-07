#pragma once

#include "common.pb.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

bool convert_pb_replica_status_to_phase(pb::ReplicaStatus in, ReplicaPhase& out);
}  // namespace adviskv::sdm