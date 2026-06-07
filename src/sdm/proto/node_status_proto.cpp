#include "sdm/proto/node_status_proto.h"

#include "common/define.h"

namespace adviskv::sdm {

bool decode_pb_node_status(pb::NodeStatus in, NodeStatus& out) {
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

bool encode_pb_node_status(NodeStatus in, pb::NodeStatus& out) {
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

}  // namespace adviskv::sdm
