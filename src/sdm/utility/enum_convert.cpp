#include "sdm/utility/enum_convert.h"

#include "common/define.h"

namespace adviskv::sdm {

bool convert_pb_to_node_status(pb::NodeStatus in, NodeStatus& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL2(out, pb::NodeStatus, NODE_ONLINE, NodeStatus,
                           ONLINE)
        SWITCH_TYPE_EQUAL2(out, pb::NodeStatus, NODE_OFFLINE, NodeStatus,
                           OFFLINE)
        SWITCH_TYPE_EQUAL2(out, pb::NodeStatus, NODE_SUSPECT, NodeStatus,
                           SUSPECT)
        default:
            return false;
    }
    return true;
}

bool convert_node_status_to_pb(NodeStatus in, pb::NodeStatus& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL2(out, NodeStatus, ONLINE, pb::NodeStatus,
                           NODE_ONLINE)
        SWITCH_TYPE_EQUAL2(out, NodeStatus, OFFLINE, pb::NodeStatus,
                           NODE_OFFLINE)
        SWITCH_TYPE_EQUAL2(out, NodeStatus, SUSPECT, pb::NodeStatus,
                           NODE_SUSPECT)
        default:
            return false;
    }
    return true;
}

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

bool convert_replica_status_to_phase(ReplicaStatus in, ReplicaPhase& out) {
    switch (in) {
        SWITCH_TYPE_EQUAL2(out, ReplicaStatus, ADDING, ReplicaPhase, CREATING)
        SWITCH_TYPE_EQUAL(out, ReplicaStatus, ReplicaPhase, READY)
        SWITCH_TYPE_EQUAL(out, ReplicaStatus, ReplicaPhase, LOST)
        SWITCH_TYPE_EQUAL(out, ReplicaStatus, ReplicaPhase, ERROR)
        default:
            return false;
    }
    return true;
}

}  // namespace adviskv::sdm
