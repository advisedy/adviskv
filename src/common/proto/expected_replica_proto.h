#pragma once

#include "common/define.h"
#include "sdm.pb.h"
#include "sdm/model/service_param.h"

namespace adviskv {

inline bool encode_pb_expected_replica_type(
    sdm::ExpectedReplicaType in, rpc::ExpectedReplicaType& out) {
    switch (in) {
        case sdm::ExpectedReplicaType::PRESENT:
            out = rpc::PRESENT;
            return true;
        case sdm::ExpectedReplicaType::ABSENT:
            out = rpc::ABSENT;
            return true;
        case sdm::ExpectedReplicaType::ADD_MEMBER:
            out = rpc::ADD_MEMBER;
            return true;
        case sdm::ExpectedReplicaType::REMOVE_MEMBER:
            out = rpc::REMOVE_MEMBER;
            return true;
        default:
            return false;
    }
}

inline rpc::ExpectedReplicaType to_pb_expected_replica_type(
    sdm::ExpectedReplicaType type) {
    rpc::ExpectedReplicaType out = rpc::ABSENT;
    IGNORE_RESULT(encode_pb_expected_replica_type(type, out));
    return out;
}

}  // namespace adviskv