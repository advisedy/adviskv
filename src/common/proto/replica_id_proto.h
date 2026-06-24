#pragma once

#include "common.pb.h"
#include "common/type.h"

namespace adviskv {

inline void encode_pb_replica_id(const ReplicaID& in, pb::ReplicaID& out) {
    out.set_table_id(in.table_id);
    out.set_shard_index(in.shard_index);
    out.set_replica_seq(in.replica_seq);
}

inline ReplicaID decode_pb_replica_id(const pb::ReplicaID& in) {
    return ReplicaID{in.table_id(), in.shard_index(), in.replica_seq()};
}

inline void encode_pb_endpoint(const Endpoint& in, pb::Endpoint& out) {
    out.set_ip(in.ip);
    out.set_port(in.port);
}

inline Endpoint decode_pb_endpoint(const pb::Endpoint& in) {
    return Endpoint{in.ip(), in.port()};
}

inline void encode_pb_peer_member(const PeerMember& in, pb::PeerMember& out) {
    encode_pb_replica_id(in.replica_id, *out.mutable_replica_id());
    out.set_node_id(in.node_id);
    encode_pb_endpoint(in.endpoint, *out.mutable_endpoint());
}

inline PeerMember decode_pb_peer_member(const pb::PeerMember& in) {
    return PeerMember{
        in.node_id(),
        decode_pb_replica_id(in.replica_id()),
        decode_pb_endpoint(in.endpoint()),
    };
}

}  // namespace adviskv