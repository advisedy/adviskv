#pragma once

#include "common.pb.h"
#include "common/model/peer_member.h"
#include "common/proto/endpoint_proto.h"
#include "common/proto/ids_proto.h"

namespace adviskv {

inline bool encode_pb_peer_member(const PeerMember& in, pb::PeerMember& out) {
    if (!encode_pb_replica_id(in.replica_id, *out.mutable_replica_id())) {
        return false;
    }
    out.set_node_id(in.node_id);
    if (!encode_pb_endpoint(in.endpoint, *out.mutable_endpoint())) {
        return false;
    }
    return true;
}

inline bool decode_pb_peer_member(const pb::PeerMember& in, PeerMember& out) {
    out.node_id = in.node_id();
    if (!decode_pb_replica_id(in.replica_id(), out.replica_id)) {
        return false;
    }
    if (!decode_pb_endpoint(in.endpoint(), out.endpoint)) {
        return false;
    }
    return true;
}

}  // namespace adviskv
