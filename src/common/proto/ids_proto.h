#pragma once

#include "common.pb.h"
#include "common/model/ids.h"

namespace adviskv {

inline bool encode_pb_replica_id(const ReplicaID& in, pb::ReplicaID& out) {
    out.set_table_id(in.table_id);
    out.set_shard_index(in.shard_index);
    out.set_replica_seq(in.replica_seq);
    return true;
}

inline bool decode_pb_replica_id(const pb::ReplicaID& in, ReplicaID& out) {
    out = ReplicaID{in.table_id(), in.shard_index(), in.replica_seq()};
    return true;
}

}  // namespace adviskv
