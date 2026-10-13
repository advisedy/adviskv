#pragma once

#include <cassert>
#include <cstdint>
#include <type_traits>
#include <utility>

#include <fmt/core.h>
#include <fmt/format.h>

#include "common.pb.h"
#include "meta.pb.h"
#include "storage.pb.h"

#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"

namespace adviskv {

namespace meta_rpc = ::adviskv::meta::rpc;
namespace storage_rpc = ::adviskv::rpc;

namespace codec {

namespace {

inline bool try_decode_status_code(int32_t code, StatusCode& out) {
    switch (code) {
#define X(name, value)          \
    case value:                 \
        out = StatusCode::name; \
        return true;
        ADVISKV_STATUS_CODE_LIST(X)
#undef X
        default:
            return false;
    }
}

}  // namespace

inline void encode(const Status& in, pb::BaseRsp& out) {
    out.set_code(to_rpc_code(in.code()));
    out.set_msg(in.msg());
}

inline bool decode(const pb::BaseRsp& in, Status& out) {
    StatusCode code = StatusCode::ERROR;
    if (!try_decode_status_code(in.code(), code)) {
        return false;
    }
    out = Status{code, in.msg()};
    return true;
}

inline void encode(const Endpoint& in, pb::Endpoint& out) {
    out.set_ip(in.ip);
    out.set_port(in.port);
}

inline bool decode(const pb::Endpoint& in, Endpoint& out) {
    out = Endpoint{in.ip(), in.port()};
    return true;
}

inline void encode(const ReplicaID& in, pb::ReplicaID& out) {
    out.set_table_id(in.table_id);
    out.set_shard_index(in.shard_index);
    out.set_replica_seq(in.replica_seq);
}

inline bool decode(const pb::ReplicaID& in, ReplicaID& out) {
    out = ReplicaID{in.table_id(), in.shard_index(), in.replica_seq()};
    return true;
}

inline void encode(const PeerMember& in, pb::PeerMember& out) {
    encode(in.replica_id, *out.mutable_replica_id());
    out.set_node_id(in.node_id);
    encode(in.endpoint, *out.mutable_endpoint());
}

inline bool decode(const pb::PeerMember& in, PeerMember& out) {
    out.node_id = in.node_id();
    if (!decode(in.replica_id(), out.replica_id)) {
        return false;
    }
    if (!decode(in.endpoint(), out.endpoint)) {
        return false;
    }
    return true;
}

inline void encode(const RaftMember& in, pb::RaftMember& out) {
    encode(in.peer, *out.mutable_peer());
    out.set_member_type(static_cast<int32_t>(in.member_type));
}

inline bool decode(const pb::RaftMember& in, RaftMember& out) {
    if (!decode(in.peer(), out.peer)) {
        return false;
    }
    if (!is_valid_enum_value<RaftMemberType>(in.member_type())) {
        return false;
    }
    out.member_type = static_cast<RaftMemberType>(in.member_type());
    return true;
}

inline void encode(const ExpectedReplica& in, pb::ExpectedReplica& out) {
    out.set_type(static_cast<int32_t>(in.type));
    encode(in.replica_id, *out.mutable_replica_id());
    out.set_engine_type(static_cast<int32_t>(in.engine_type));
    for (const PeerMember& member : in.initial_members) {
        encode(member, *out.add_initial_members());
    }
}

inline bool decode(const pb::ExpectedReplica& in, ExpectedReplica& out) {
    if (!decode(in.replica_id(), out.replica_id)) {
        return false;
    }
    if (!is_valid_enum_value<ExpectedReplicaType>(in.type())) {
        return false;
    }
    out.type = static_cast<ExpectedReplicaType>(in.type());
    if (!is_valid_enum_value<EngineType>(in.engine_type())) {
        return false;
    }
    out.engine_type = static_cast<EngineType>(in.engine_type());
    out.initial_members.clear();
    for (const auto& member_pb : in.initial_members()) {
        PeerMember member;
        if (!decode(member_pb, member)) {
            return false;
        }
        out.initial_members.push_back(std::move(member));
    }
    return true;
}

inline void encode(const LogEntry& in, pb::LogEntry& out) {
    out.set_index(in.index);
    out.set_term(in.term);
    out.set_op_type(static_cast<int32_t>(in.op_type));
    out.set_key(in.key);
    out.set_value(in.value);
    out.set_expected(in.expected);
    out.set_request_id(in.request_id);
    encode(in.config_member, *out.mutable_config_member());
    encode(in.config_replica_id, *out.mutable_config_replica_id());
}

inline bool decode(const pb::LogEntry& in, LogEntry& out) {
    out = {};
    out.term = in.term();
    out.index = in.index();
    if (!is_valid_enum_value<WriteOpType>(in.op_type())) {
        return false;
    }
    out.op_type = static_cast<WriteOpType>(in.op_type());
    out.key = in.key();
    out.value = in.value();
    out.expected = in.expected();
    out.request_id = in.request_id();
    if (!decode(in.config_member(), out.config_member)) {
        return false;
    }
    if (!decode(in.config_replica_id(), out.config_replica_id)) {
        return false;
    }
    return true;
}

inline void encode(const HashRange& in, pb::HashRange& out) {
    out.set_start(in.start);
    out.set_end(in.end);
}

inline bool decode(const pb::HashRange& in, HashRange& out) {
    out.start = in.start();
    out.end = in.end();
    return true;
}

inline void encode(const RouteReplica& in, pb::RouteReplica& out) {
    encode(in.replica_id, *out.mutable_replica_id());
    encode(in.endpoint, *out.mutable_endpoint());
    out.set_role(static_cast<int32_t>(in.role));
}

inline bool decode(const pb::RouteReplica& in, RouteReplica& out) {
    if (!decode(in.replica_id(), out.replica_id)) {
        return false;
    }
    if (!decode(in.endpoint(), out.endpoint)) {
        return false;
    }
    if (!is_valid_enum_value<ReplicaRole>(in.role())) {
        return false;
    }
    out.role = static_cast<ReplicaRole>(in.role());
    return true;
}

inline void encode(const ShardRoute& in, pb::ShardRoute& out) {
    out.set_shard_index(in.shard_id.shard_index);
    for (const RouteReplica& replica : in.replicas) {
        encode(replica, *out.add_replicas());
    }
    encode(in.hash_range, *out.mutable_hash_range());
}

inline bool decode(const pb::ShardRoute& in, ShardRoute& out) {
    out.shard_id.shard_index = in.shard_index();
    out.replicas.clear();
    for (const auto& replica_pb : in.replicas()) {
        RouteReplica replica;
        if (!decode(replica_pb, replica)) {
            return false;
        }
        out.replicas.push_back(std::move(replica));
    }
    if (in.has_hash_range()) {
        if (!decode(in.hash_range(), out.hash_range)) {
            return false;
        }
    }
    return true;
}


inline bool decode(const pb::HeartbeatReplicaInfo& in, HeartBeatReplicaInfo& out) {
    if (!decode(in.replica_id(), out.replica_id)) {
        return false;
    }
    if (!is_valid_enum_value<ReplicaRole>(in.role())) {
        return false;
    }
    out.role = static_cast<ReplicaRole>(in.role());
    if (!is_valid_enum_value<StorageReplicaStatus>(in.status())) {
        return false;
    }
    out.storage_status = static_cast<StorageReplicaStatus>(in.status());
    out.term = in.term();
    out.full_membership.clear();
    for (const auto& member_pb : in.full_membership()) {
        RaftMember member;
        if (!decode(member_pb, member)) {
            return false;
        }
        out.full_membership.push_back(std::move(member));
    }
    return true;
}

inline Status base_rsp_to_status(const pb::BaseRsp& rsp) {
    Status status;
    if (!decode(rsp, status)) {
        return Status::ERROR(fmt::format("unknown remote status code={}, msg={}", rsp.code(), rsp.msg()));
    }
    return status;
}

// 要求 Response 带 mutable_base_rsp()
template <typename Response>
inline void status_to_base_rsp(Response* rsp, const Status& status) {
    static_assert(std::is_same_v<std::remove_pointer_t<decltype(std::declval<Response*>()->mutable_base_rsp())>,
                                 pb::BaseRsp>,
                  "Response must embed adviskv.pb.BaseRsp and expose mutable_base_rsp()");
    assert(rsp != nullptr);
    if (rsp == nullptr) return;
    encode(status, *rsp->mutable_base_rsp());
}

}  // namespace codec

}  // namespace adviskv
