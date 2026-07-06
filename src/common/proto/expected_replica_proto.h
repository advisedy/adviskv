#pragma once

#include <utility>

#include "common/define.h"
#include "common/model/expected_replica.h"
#include "common/proto/engine_type_proto.h"
#include "common/proto/ids_proto.h"
#include "common/proto/peer_member_proto.h"
#include "common/proto/rpc_alias.h"

namespace adviskv {

inline bool encode_pb_expected_replica_type(ExpectedReplicaType in, sdm_rpc::ExpectedReplicaType& out) {
    switch (in) {
        case ExpectedReplicaType::PRESENT:
            out = sdm_rpc::PRESENT;
            return true;
        case ExpectedReplicaType::ABSENT:
            out = sdm_rpc::ABSENT;
            return true;
        case ExpectedReplicaType::ADD_MEMBER:
            out = sdm_rpc::ADD_MEMBER;
            return true;
        case ExpectedReplicaType::REMOVE_MEMBER:
            out = sdm_rpc::REMOVE_MEMBER;
            return true;
        default:
            return false;
    }
}

inline bool decode_pb_expected_replica_type(sdm_rpc::ExpectedReplicaType in, ExpectedReplicaType& out) {
    switch (in) {
        case sdm_rpc::PRESENT:
            out = ExpectedReplicaType::PRESENT;
            return true;
        case sdm_rpc::ABSENT:
            out = ExpectedReplicaType::ABSENT;
            return true;
        case sdm_rpc::ADD_MEMBER:
            out = ExpectedReplicaType::ADD_MEMBER;
            return true;
        case sdm_rpc::REMOVE_MEMBER:
            out = ExpectedReplicaType::REMOVE_MEMBER;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_expected_replica(const ExpectedReplica& in, sdm_rpc::ExpectedReplica& out) {
    sdm_rpc::ExpectedReplicaType type = sdm_rpc::ABSENT;
    if (!encode_pb_expected_replica_type(in.type, type)) {
        return false;
    }
    out.set_type(type);
    if (!encode_pb_replica_id(in.replica_id, *out.mutable_replica_id())) {
        return false;
    }
    pb::EngineType engine_type = pb::ENGINE_TYPE_UNSPECIFIED;
    if (!encode_pb_engine_type(in.engine_type, engine_type)) {
        return false;
    }
    out.set_engine_type(engine_type);
    for (const PeerMember& member : in.initial_members) {
        if (!encode_pb_peer_member(member, *out.add_initial_members())) {
            return false;
        }
    }
    return true;
}

inline bool decode_pb_expected_replica(const sdm_rpc::ExpectedReplica& in, ExpectedReplica& out) {
    if (!decode_pb_replica_id(in.replica_id(), out.replica_id)) {
        return false;
    }
    if (!decode_pb_expected_replica_type(in.type(), out.type)) {
        return false;
    }
    if (!decode_pb_engine_type(in.engine_type(), out.engine_type)) {
        return false;
    }
    out.initial_members.clear();
    for (const auto& member_pb : in.initial_members()) {
        PeerMember member;
        if (!decode_pb_peer_member(member_pb, member)) {
            return false;
        }
        out.initial_members.push_back(std::move(member));
    }
    return true;
}

}  // namespace adviskv