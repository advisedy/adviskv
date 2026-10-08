#pragma once

#include <fmt/format.h>

#include "common/binary/codec.h"
#include "common/define.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/persist/replica_meta_persist_engine.h"

namespace adviskv {
namespace binary {

template <>
struct BinaryCodec<storage::RaftMeta> : std::true_type {
    static Status encode(const storage::RaftMeta& meta, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(meta.current_term, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(meta.voted_for.has_value(), writer))
        if (meta.voted_for.has_value()) {
            RETURN_IF_INVALID_STATUS(binary::encode(meta.voted_for.value(), writer))
        }
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, storage::RaftMeta& meta) {
        meta = {};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, meta.current_term))
        bool has_voted_for{false};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, has_voted_for))
        if (has_voted_for) {
            ReplicaID replica_id;
            RETURN_IF_INVALID_STATUS(binary::decode(reader, replica_id))
            meta.voted_for = replica_id;
        }
        return Status::OK();
    }
};


template <>
struct BinaryCodec<storage::ReplicaMetaPayload> : std::true_type {
    static Status encode(const storage::ReplicaMetaPayload& meta, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(meta.init_param.replica_id, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(meta.init_param.engine_type, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(meta.init_param.local_endpoint, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(static_cast<int32>(meta.init_param.members.size()), writer))
        for (const PeerMember& member : meta.init_param.members) {
            RETURN_IF_INVALID_STATUS(binary::encode(member, writer))
        }
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, storage::ReplicaMetaPayload& meta) {
        meta = {};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, meta.init_param.replica_id))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, meta.init_param.engine_type))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, meta.init_param.local_endpoint))

        int32 member_count{0};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, member_count))
        if (member_count < 0) {
            return Status::ERROR(fmt::format("invalid replica member count: {}", member_count));
        }
        meta.init_param.members.clear();
        meta.init_param.members.reserve(static_cast<size_t>(member_count));
        for (int32 i = 0; i < member_count; ++i) {
            PeerMember member;
            RETURN_IF_INVALID_STATUS(binary::decode(reader, member))
            meta.init_param.members.push_back(std::move(member));
        }
        return Status::OK();
    }
};



template <>
struct BinaryCodec<storage::RequestRecord> : std::true_type {
    static Status encode(const storage::RequestRecord& record, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(record.request_id, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(record.op, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(record.payload_hash, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(static_cast<int32>(record.apply_status.code()), writer))
        RETURN_IF_INVALID_STATUS(binary::encode(record.apply_status.msg(), writer))
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, storage::RequestRecord& record) {
        record = {};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, record.request_id))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, record.op))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, record.payload_hash))
        int32 status_code{0};
        std::string status_msg;
        RETURN_IF_INVALID_STATUS(binary::decode(reader, status_code))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, status_msg))
        record.apply_status = Status{status_code, status_msg};
        return Status::OK();
    }
};

}  // namespace binary
}  // namespace adviskv