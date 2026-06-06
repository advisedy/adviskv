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

pb::ReplicaRole to_pb_replica_role(ReplicaRole role) {
    pb::ReplicaRole out{pb::ReplicaRole::FOLLOWER};
    IGNORE_RESULT(convert_replica_role_to_pb(role, out))
    return out;
}

pb::ReplicaStatus to_pb_replica_status(storage::ReplicaStatus status) {
    switch (status) {
        case storage::ReplicaStatus::INITIALIZING:
        case storage::ReplicaStatus::RECOVERING:
            return pb::ReplicaStatus::ADDING;
        case storage::ReplicaStatus::READY:
            return pb::ReplicaStatus::READY;
        case storage::ReplicaStatus::FAULTED:
        default:
            return pb::ReplicaStatus::ERROR;
    }
}

}  // namespace adviskv
