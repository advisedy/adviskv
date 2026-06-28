#pragma once

#include "common/define.h"
#include "common/model/expected_replica.h"
#include "common/proto/replica_id_proto.h"
#include "sdm.pb.h"

namespace adviskv {

inline bool encode_pb_expected_replica_type(ExpectedReplicaType in,
                                            rpc::ExpectedReplicaType& out) {
    switch (in) {
        case ExpectedReplicaType::PRESENT:
            out = rpc::PRESENT;
            return true;
        case ExpectedReplicaType::ABSENT:
            out = rpc::ABSENT;
            return true;
        case ExpectedReplicaType::ADD_MEMBER:
            out = rpc::ADD_MEMBER;
            return true;
        case ExpectedReplicaType::REMOVE_MEMBER:
            out = rpc::REMOVE_MEMBER;
            return true;
        default:
            return false;
    }
}

inline bool decode_pb_expected_replica_type(rpc::ExpectedReplicaType in,
                                            ExpectedReplicaType& out) {
    switch (in) {
        case rpc::PRESENT:
            out = ExpectedReplicaType::PRESENT;
            return true;
        case rpc::ABSENT:
            out = ExpectedReplicaType::ABSENT;
            return true;
        case rpc::ADD_MEMBER:
            out = ExpectedReplicaType::ADD_MEMBER;
            return true;
        case rpc::REMOVE_MEMBER:
            out = ExpectedReplicaType::REMOVE_MEMBER;
            return true;
        default:
            return false;
    }
}

inline rpc::ExpectedReplicaType to_pb_expected_replica_type(
    ExpectedReplicaType type) {
    rpc::ExpectedReplicaType out = rpc::ABSENT;
    IGNORE_RESULT(encode_pb_expected_replica_type(type, out));
    return out;
}

inline void encode_pb_expected_replica(const ExpectedReplica& in,
                                       rpc::ExpectedReplica& out) {
    out.set_type(to_pb_expected_replica_type(in.type));
    encode_pb_replica_id(in.replica_id, *out.mutable_replica_id());
    out.set_engine_type(to<int8>(in.engine_type));
    for (const PeerMember& member : in.initial_members) {
        encode_pb_peer_member(member, *out.add_initial_members());
    }
}

inline ExpectedReplica decode_pb_expected_replica(
    const rpc::ExpectedReplica& in) {
    ExpectedReplica out;
    out.replica_id = decode_pb_replica_id(in.replica_id());
    IGNORE_RESULT(decode_pb_expected_replica_type(in.type(), out.type));
    out.engine_type = static_cast<EngineType>(in.engine_type());
    for (const auto& member_pb : in.initial_members()) {
        out.initial_members.push_back(decode_pb_peer_member(member_pb));
    }
    return out;
}

}  // namespace adviskv