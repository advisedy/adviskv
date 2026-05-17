#include "common/enum_convert.h"

#include "common/define.h"

namespace adviskv {

bool convert_pb_to_replica_role(pb::ReplicaRole in, ReplicaRole& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL(out, pb::ReplicaRole, ReplicaRole, LEADER)
        SWITCH_TYPE_EQUAL(out, pb::ReplicaRole, ReplicaRole, FOLLOWER)
        default:
            return false;
    }
    return true;
}

bool convert_replica_role_to_pb(ReplicaRole in, pb::ReplicaRole& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL(out, ReplicaRole, pb::ReplicaRole, LEADER)
        SWITCH_TYPE_EQUAL(out, ReplicaRole, pb::ReplicaRole, FOLLOWER)
        case ReplicaRole::CANDIDATE:
            out = pb::ReplicaRole::FOLLOWER;
            break;
        default:
            return false;
    }
    return true;
}

bool convert_pb_to_replica_status(pb::ReplicaStatus in, ReplicaStatus& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL(out, pb::ReplicaStatus, ReplicaStatus, ADDING)
        SWITCH_TYPE_EQUAL(out, pb::ReplicaStatus, ReplicaStatus, READY)
        SWITCH_TYPE_EQUAL(out, pb::ReplicaStatus, ReplicaStatus, LOST)
        SWITCH_TYPE_EQUAL(out, pb::ReplicaStatus, ReplicaStatus, ERROR)
        default:
            return false;
    }
    return true;
}

bool convert_replica_status_to_pb(ReplicaStatus in, pb::ReplicaStatus& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL(out, ReplicaStatus, pb::ReplicaStatus, ADDING)
        SWITCH_TYPE_EQUAL(out, ReplicaStatus, pb::ReplicaStatus, READY)
        SWITCH_TYPE_EQUAL(out, ReplicaStatus, pb::ReplicaStatus, LOST)
        SWITCH_TYPE_EQUAL(out, ReplicaStatus, pb::ReplicaStatus, ERROR)
        default:
            return false;
    }
    return true;
}

pb::ReplicaRole to_pb_replica_role(ReplicaRole role) {
    pb::ReplicaRole out{pb::ReplicaRole::FOLLOWER};
    IGNORE_RESULT(convert_replica_role_to_pb(role, out))
    return out;
}

pb::ReplicaStatus to_pb_replica_status(ReplicaStatus status) {
    pb::ReplicaStatus out{pb::ReplicaStatus::ERROR};
    IGNORE_RESULT(convert_replica_status_to_pb(status, out))
    return out;
}

}  // namespace adviskv
