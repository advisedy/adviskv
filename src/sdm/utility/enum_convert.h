#pragma once

#include "common.pb.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

bool convert_pb_to_node_status(pb::NodeStatus in, sdm::NodeStatus& out);
bool convert_node_status_to_pb(sdm::NodeStatus in, pb::NodeStatus& out);
bool convert_pb_replica_status_to_phase(pb::ReplicaStatus in, ReplicaPhase& out);

// ######################################################
//  提供给外部的接口
#define CONVERT_PB_TO_NODE_STATUS(in, out)                               \
    {                                                                    \
        bool res = convert_pb_to_node_status(in, out);                   \
        if (!res) {                                                      \
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, \
                                           "node_status is not valid"}); \
            return grpc::Status::OK;                                     \
        }                                                                \
    }
}  // namespace adviskv::sdm