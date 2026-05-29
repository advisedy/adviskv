#include "storage/persist/replica_meta_persist_engine.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/buffer.h"
#include "common/defer.h"
#include "common/define.h"
#include "common/framed_record_codec.h"
#include "common/func.h"

namespace adviskv::storage {
namespace {

constexpr int64 kMaxReplicaMetaPayloadBytes = 1024 * 1024;

class ReplicaMetaCodec {
   public:
    using ObjectType = ReplicaMetaPayload;
    using LenType = int64;

    LenType max_payload_len() const { return kMaxReplicaMetaPayloadBytes; }

    void encode_payload(const ObjectType& meta, EncodeBuffer& buf) const {
        buf.write(meta.init_param.replica_id.table_id);
        buf.write(meta.init_param.replica_id.shard_index);
        buf.write(meta.init_param.replica_id.replica_index);
        buf.write(static_cast<int32>(meta.init_param.engine_type));
        buf.write(meta.init_param.local_endpoint.ip);
        buf.write(meta.init_param.local_endpoint.port);
        buf.write<int32>(static_cast<int32>(meta.init_param.members.size()));
        for (const PeerMember& member : meta.init_param.members) {
            buf.write(member.node_id);
            buf.write(member.replica_id.table_id);
            buf.write(member.replica_id.shard_index);
            buf.write(member.replica_id.replica_index);
            buf.write(member.endpoint.ip);
            buf.write(member.endpoint.port);
        }
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& meta) const {
        meta = {};
        RETURN_IF_INVALID_READ(buf, meta.init_param.replica_id.table_id)
        RETURN_IF_INVALID_READ(buf, meta.init_param.replica_id.shard_index)
        RETURN_IF_INVALID_READ(buf, meta.init_param.replica_id.replica_index)
        int32 engine_type{0};
        RETURN_IF_INVALID_READ(buf, engine_type)
        meta.init_param.engine_type = static_cast<EngineType>(engine_type);
        RETURN_IF_INVALID_READ(buf, meta.init_param.local_endpoint.ip)
        RETURN_IF_INVALID_READ(buf, meta.init_param.local_endpoint.port)

        int32 member_count{0};
        RETURN_IF_INVALID_READ(buf, member_count)
        if (member_count < 0) {
            return Status::ERROR(
                fmt::format("invalid replica member count: {}", member_count));
        }
        meta.init_param.members.clear();
        meta.init_param.members.reserve(static_cast<size_t>(member_count));
        for (int32 i = 0; i < member_count; ++i) {
            PeerMember member;
            RETURN_IF_INVALID_READ(buf, member.node_id)
            RETURN_IF_INVALID_READ(buf, member.replica_id.table_id)
            RETURN_IF_INVALID_READ(buf, member.replica_id.shard_index)
            RETURN_IF_INVALID_READ(buf, member.replica_id.replica_index)
            RETURN_IF_INVALID_READ(buf, member.endpoint.ip)
            RETURN_IF_INVALID_READ(buf, member.endpoint.port)
            meta.init_param.members.push_back(std::move(member));
        }
        return Status::OK();
    }
};

}  // namespace

ReplicaMetaPersistEngine::ReplicaMetaPersistEngine(std::string data_dir)
    : data_dir_(std::move(data_dir)) {}

std::filesystem::path ReplicaMetaPersistEngine::replica_dir(
    const ReplicaID& replica_id) const {
    return std::filesystem::path(data_dir_) /
           fmt::format("{}-{}", replica_id.table_id, replica_id.shard_index);
}

std::filesystem::path ReplicaMetaPersistEngine::meta_path(
    const ReplicaID& replica_id) const {
    return replica_dir(replica_id) / kFileName;
}

Status ReplicaMetaPersistEngine::save_replica_meta(
    const ReplicaMetaPayload& payload) const {
    std::filesystem::path dir = replica_dir(payload.init_param.replica_id);
    std::filesystem::path path = meta_path(payload.init_param.replica_id);

    return func::atomic_replace_file(path, [&payload](int fd) {
        return FramedRecord<ReplicaMetaCodec>::encode_to_fd(fd, payload);
    });
}

Status ReplicaMetaPersistEngine::load_replica_meta(
    const std::filesystem::path& path, ReplicaMetaPayload& payload) const {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::ERROR(
            fmt::format("open replica meta failed: {}", path.string()));
    }
    auto close_fd = Defer([&]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    RETURN_IF_INVALID_STATUS(FramedRecord<ReplicaMetaCodec>::decode_from_fd(fd, payload))
    payload.init_param.data_dir = data_dir_;

    return Status::OK();
}

Status ReplicaMetaPersistEngine::load_replica_meta(
    const ReplicaID& replica_id, ReplicaMetaPayload& payload) const {
    return load_replica_meta(meta_path(replica_id), payload);
}

Status ReplicaMetaPersistEngine::delete_replica_meta(
    const ReplicaID& replica_id) const {
    std::filesystem::path path = meta_path(replica_id);
    try {
        if (!std::filesystem::exists(path)) {
            return Status::OK();
        }
        std::filesystem::remove(path);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::ERROR(
            fmt::format("delete replica meta failed: {}", e.what()));
    }
}

// 返回所有的meta的路径，用来调用load_replica_meta
std::vector<std::filesystem::path>
ReplicaMetaPersistEngine::scan_replica_meta_files() const {
    std::vector<std::filesystem::path> paths;
    if (!std::filesystem::exists(data_dir_)) {
        return paths;
    }
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        if (!entry.is_directory()) {
            continue;
        }
        std::filesystem::path path = entry.path() / kFileName;
        if (std::filesystem::exists(path)) {
            paths.push_back(std::move(path));
        }
    }
    std::sort(paths.begin(), paths.end());  // 这里sort一下，方便测试啥的。
    return paths;
}

}  // namespace adviskv::storage
