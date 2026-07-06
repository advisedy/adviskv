#pragma once

#include "common.pb.h"
#include "common/model/raft_member.h"
#include "common/proto/peer_member_proto.h"
#include "common/proto/raft_member_type_proto.h"

namespace adviskv {

inline bool encode_pb_raft_member(const RaftMember& in, pb::RaftMember& out) {
    if (!encode_pb_peer_member(in.peer, *out.mutable_peer())) {
        return false;
    }
    pb::RaftMemberType member_type = pb::RaftMemberType::RAFT_MEMBER_TYPE_NON_MEMBER;
    if (!encode_pb_raft_member_type(in.member_type, member_type)) {
        return false;
    }
    out.set_member_type(member_type);
    return true;
}

inline bool decode_pb_raft_member(const pb::RaftMember& in, RaftMember& out) {
    if (!decode_pb_peer_member(in.peer(), out.peer)) {
        return false;
    }
    return decode_pb_raft_member_type(in.member_type(), out.member_type);
}

}  // namespace adviskv
