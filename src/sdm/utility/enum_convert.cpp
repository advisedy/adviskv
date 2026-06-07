#include "sdm/utility/enum_convert.h"

#include "common/define.h"

namespace adviskv::sdm {

bool convert_pb_replica_status_to_phase(pb::ReplicaStatus in, ReplicaPhase& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL2(out, pb::ReplicaStatus, ADDING, ReplicaPhase,
                           CREATING)
        SWITCH_TYPE_EQUAL(out, pb::ReplicaStatus, ReplicaPhase, READY)
        SWITCH_TYPE_EQUAL(out, pb::ReplicaStatus, ReplicaPhase, LOST)
        SWITCH_TYPE_EQUAL(out, pb::ReplicaStatus, ReplicaPhase, ERROR)
        default:
            return false;
    }
    return true;
}

}  // namespace adviskv::sdm