#pragma once

#include "common/enum_convert.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

using adviskv::convert_pb_to_replica_status;
using adviskv::convert_pb_to_replica_role;
using adviskv::convert_replica_role_to_pb;
using adviskv::convert_replica_status_to_pb;

bool convert_pb_to_node_status(pb::NodeStatus in, sdm::NodeStatus& out);
bool convert_node_status_to_pb(sdm::NodeStatus in, pb::NodeStatus& out);
bool convert_replica_status_to_phase(ReplicaStatus in, ReplicaPhase& out);

// ######################################################
//  提供给外部的接口
#define CONVERT_PB_TO_REPLICA_STATUS(in, out)                               \
    {                                                                       \
        bool res = convert_pb_to_replica_status(in, out);                   \
        if (!res) {                                                         \
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT,    \
                                           "replica status is not valid"}); \
            return grpc::Status::OK;                                        \
        }                                                                   \
    }

#define CONVERT_PB_TO_NODE_STATUS(in, out)                               \
    {                                                                    \
        bool res = convert_pb_to_node_status(in, out);                   \
        if (!res) {                                                      \
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, \
                                           "node_status is not valid"}); \
            return grpc::Status::OK;                                     \
        }                                                                \
    }

#define CONVERT_PB_TO_REPLICA_ROLE(in, out)                               \
    {                                                                     \
        bool res = convert_pb_to_replica_role(in, out);                   \
        if (!res) {                                                       \
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT,  \
                                           "replica role is not valid"}); \
            return grpc::Status::OK;                                      \
        }                                                                 \
    }

#define CONVERT_REPLICA_ROLE_TO_PB(in, out)                               \
    {                                                                     \
        bool res = convert_replica_role_to_pb(in, out);                   \
        if (!res) {                                                       \
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT,  \
                                           "replica role is not valid"}); \
            return grpc::Status::OK;                                      \
        }                                                                 \
    }

}  // namespace adviskv::sdm
