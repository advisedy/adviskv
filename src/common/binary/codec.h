#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

#include "common/binary/base.h"
#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"

namespace adviskv {
namespace binary {

template <typename T>
struct BinaryCodec : std::false_type {};

namespace detail {

template <typename T>
using normalized_t = std::remove_cv_t<std::remove_reference_t<T>>;

template <typename T>
inline constexpr bool is_registered_v = BinaryCodec<normalized_t<T>>::value;

}  // namespace detail

template <typename T, std::enable_if_t<detail::is_registered_v<T>, int> = 0>
Status encode(const T& value, BinaryWriter& writer);

template <typename T, std::enable_if_t<detail::is_registered_v<T>, int> = 0>
Status decode(BinaryReader& reader, T& value);

template <>
struct BinaryCodec<int32_t> : std::true_type {
    static Status encode(const int32_t& value, BinaryWriter& writer) {
        return writer.write_raw(&value, sizeof(value));
    }

    static Status decode(BinaryReader& reader, int32_t& value) {
        RETURN_IF_INVALID_STATUS(reader.read_raw(&value, sizeof(value)))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<int64_t> : std::true_type {
    static Status encode(const int64_t& value, BinaryWriter& writer) {
        return writer.write_raw(&value, sizeof(value));
    }

    static Status decode(BinaryReader& reader, int64_t& value) {
        RETURN_IF_INVALID_STATUS(reader.read_raw(&value, sizeof(value)))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<uint32_t> : std::true_type {
    static Status encode(const uint32_t& value, BinaryWriter& writer) {
        return writer.write_raw(&value, sizeof(value));
    }

    static Status decode(BinaryReader& reader, uint32_t& value) {
        RETURN_IF_INVALID_STATUS(reader.read_raw(&value, sizeof(value)))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<uint64_t> : std::true_type {
    static Status encode(const uint64_t& value, BinaryWriter& writer) {
        return writer.write_raw(&value, sizeof(value));
    }

    static Status decode(BinaryReader& reader, uint64_t& value) {
        RETURN_IF_INVALID_STATUS(reader.read_raw(&value, sizeof(value)))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<bool> : std::true_type {
    static Status encode(const bool& value, BinaryWriter& writer) {
        return writer.write_raw(&value, sizeof(value));
    }

    static Status decode(BinaryReader& reader, bool& value) {
        RETURN_IF_INVALID_STATUS(reader.read_raw(&value, sizeof(value)))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<std::string> : std::true_type {
    static Status encode(const std::string& value, BinaryWriter& writer) {
        int32_t len = static_cast<int32_t>(value.size());
        RETURN_IF_INVALID_STATUS(writer.write_raw(&len, sizeof(len)))
        if (!value.empty()) {
            RETURN_IF_INVALID_STATUS(writer.write_raw(value.data(), static_cast<size_t>(len)))
        }
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, std::string& value) {
        int32_t len{0};
        RETURN_IF_INVALID_STATUS(reader.read_raw(&len, sizeof(len)))
        RETURN_IF_INVALID_CONDITION(len >= 0, "invalid string len")
        value.resize(static_cast<size_t>(len));
        if (len == 0) {
            return Status::OK();
        }
        return reader.read_raw(value.data(), static_cast<size_t>(len));
    }
};

template <>
struct BinaryCodec<RaftMemberType> : std::true_type {
    static Status encode(const RaftMemberType& value, BinaryWriter& writer) {
        return binary::encode(static_cast<int32_t>(value), writer);
    }

    static Status decode(BinaryReader& reader, RaftMemberType& value) {
        int32_t raw_value{0};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, raw_value))
        RETURN_IF_INVALID_CONDITION(is_valid_enum_value<RaftMemberType>(raw_value),
                                    "invalid raft member type")
        value = static_cast<RaftMemberType>(raw_value);
        return Status::OK();
    }
};

template <>
struct BinaryCodec<EngineType> : std::true_type {
    static Status encode(const EngineType& value, BinaryWriter& writer) {
        return binary::encode(static_cast<int32_t>(value), writer);
    }

    static Status decode(BinaryReader& reader, EngineType& value) {
        int32_t raw_value{0};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, raw_value))
        RETURN_IF_INVALID_CONDITION(is_valid_enum_value<EngineType>(raw_value), "invalid engine type")
        value = static_cast<EngineType>(raw_value);
        return Status::OK();
    }
};

template <>
struct BinaryCodec<WriteOpType> : std::true_type {
    static Status encode(const WriteOpType& value, BinaryWriter& writer) {
        return binary::encode(static_cast<int32_t>(value), writer);
    }

    static Status decode(BinaryReader& reader, WriteOpType& value) {
        int32_t raw_value{0};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, raw_value))
        RETURN_IF_INVALID_CONDITION(is_valid_enum_value<WriteOpType>(raw_value), "invalid write op type")
        value = static_cast<WriteOpType>(raw_value);
        return Status::OK();
    }
};

template <>
struct BinaryCodec<Endpoint> : std::true_type {
    static Status encode(const Endpoint& value, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(value.ip, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(value.port, writer))
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, Endpoint& value) {
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.ip))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.port))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<ReplicaID> : std::true_type {
    static Status encode(const ReplicaID& value, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(value.table_id, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(value.shard_index, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(value.replica_seq, writer))
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, ReplicaID& value) {
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.table_id))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.shard_index))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.replica_seq))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<PeerMember> : std::true_type {
    static Status encode(const PeerMember& value, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(value.node_id, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(value.replica_id, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(value.endpoint, writer))
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, PeerMember& value) {
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.node_id))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.replica_id))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.endpoint))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<RaftMember> : std::true_type {
    static Status encode(const RaftMember& value, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(value.peer, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(value.member_type, writer))
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, RaftMember& value) {
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.peer))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value.member_type))
        return Status::OK();
    }
};

template <>
struct BinaryCodec<LogEntry> : std::true_type {
    static Status encode(const LogEntry& entry, BinaryWriter& writer) {
        RETURN_IF_INVALID_STATUS(binary::encode(entry.term, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.index, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.op_type, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.key, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.value, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.expected, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.config_member, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.config_replica_id, writer))
        RETURN_IF_INVALID_STATUS(binary::encode(entry.request_id, writer))
        return Status::OK();
    }

    static Status decode(BinaryReader& reader, LogEntry& entry) {
        entry = {};
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.term))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.index))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.op_type))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.key))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.value))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.expected))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.config_member))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.config_replica_id))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, entry.request_id))
        return Status::OK();
    }
};

template <typename T, std::enable_if_t<detail::is_registered_v<T>, int>>
inline Status encode(const T& value, BinaryWriter& writer) {
    return BinaryCodec<detail::normalized_t<T>>::encode(value, writer);
}

template <typename T, std::enable_if_t<detail::is_registered_v<T>, int>>
inline Status decode(BinaryReader& reader, T& value) {
    return BinaryCodec<detail::normalized_t<T>>::decode(reader, value);
}

}  // namespace binary
}  // namespace adviskv
