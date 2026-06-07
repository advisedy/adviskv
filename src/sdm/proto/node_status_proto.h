#pragma once

#include "common.pb.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

bool decode_pb_node_status(pb::NodeStatus in, NodeStatus& out);
bool encode_pb_node_status(NodeStatus in, pb::NodeStatus& out);

}  // namespace adviskv::sdm
