#pragma once

#include "common/proto/proto.h"
#include "storage.pb.h"
#include "storage/model/param.h"

namespace adviskv::storage {

inline bool decode_pb_write_op_type(int32 in, WriteOpType& out) {
    switch (in) {
        case static_cast<int32>(WriteOpType::PUT):
            out = WriteOpType::PUT;
            return true;
        case static_cast<int32>(WriteOpType::DEL):
            out = WriteOpType::DEL;
            return true;
        case static_cast<int32>(WriteOpType::NONE):
            out = WriteOpType::NONE;
            return true;
        case static_cast<int32>(WriteOpType::ADD_LEARNER):
            out = WriteOpType::ADD_LEARNER;
            return true;
        case static_cast<int32>(WriteOpType::PROMOTE_VOTER):
            out = WriteOpType::PROMOTE_VOTER;
            return true;
        case static_cast<int32>(WriteOpType::REMOVE_MEMBER):
            out = WriteOpType::REMOVE_MEMBER;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_write_op_type(WriteOpType in, int32& out) {
    switch (in) {
        case WriteOpType::PUT:
        case WriteOpType::DEL:
        case WriteOpType::NONE:
        case WriteOpType::ADD_LEARNER:
        case WriteOpType::PROMOTE_VOTER:
        case WriteOpType::REMOVE_MEMBER:
            out = static_cast<int32>(in);
            return true;
        default:
            return false;
    }
}

inline bool decode_pb_log_entry(const rpc::LogEntry& in, LogEntry& out) {
    out = {};
    out.term = in.term();
    out.index = in.index();
    if (!decode_pb_write_op_type(in.op_type(), out.op_type)) {
        return false;
    }
    out.key = in.key();
    out.value = in.value();

    if (!decode_pb_peer_member(in.config_member(), out.config_member)) {
        return false;
    }
    if (!decode_pb_replica_id(in.config_replica_id(), out.config_replica_id)) {
        return false;
    }
    return true;
}

inline bool encode_pb_log_entry(const LogEntry& in, rpc::LogEntry& out) {
    int32 op_type = 0;
    if (!encode_pb_write_op_type(in.op_type, op_type)) {
        return false;
    }

    out.set_index(in.index);
    out.set_term(in.term);
    out.set_op_type(op_type);
    out.set_key(in.key);
    out.set_value(in.value);
    if (!encode_pb_peer_member(in.config_member, *out.mutable_config_member())) {
        return false;
    }
    if (!encode_pb_replica_id(in.config_replica_id,
                              *out.mutable_config_replica_id())) {
        return false;
    }
    return true;
}

}  // namespace adviskv::storage
