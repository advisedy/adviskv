#include "storage/persist/replica_meta_persist_engine.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "common/define.h"
#include "common/file.h"
#include "common/framed_record_codec.h"
#include "storage/persist/persist_codecs.h"

namespace adviskv::storage {

ReplicaMetaPersistEngine::ReplicaMetaPersistEngine(std::string data_dir) : data_dir_(std::move(data_dir)) {}

std::filesystem::path ReplicaMetaPersistEngine::replica_dir(const ReplicaID& replica_id) const {
    return std::filesystem::path(data_dir_) / fmt::format("{}-{}", replica_id.table_id, replica_id.shard_index);
}

std::filesystem::path ReplicaMetaPersistEngine::meta_path(const ReplicaID& replica_id) const {
    return replica_dir(replica_id) / kFileName;
}

Status ReplicaMetaPersistEngine::save_replica_meta(const ReplicaMetaPayload& payload) const {
    std::filesystem::path path = meta_path(payload.init_param.replica_id);

    return File::atomic_replace(path.string(), [&payload](const File& file) {
        return FramedRecord<ReplicaMetaPayload>::encode_to(file, payload);
    });
}

Status ReplicaMetaPersistEngine::load_replica_meta(const std::filesystem::path& path,
                                                   ReplicaMetaPayload& payload) const {
    File file;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(File::open_for_read(path.string(), file, exists))
    if (!exists) {
        return Status::ERROR(fmt::format("open replica meta failed: {}", path.string()));
    }

    RETURN_IF_INVALID_STATUS(FramedRecord<ReplicaMetaPayload>::decode_from(file, payload))

    return Status::OK();
}

Status ReplicaMetaPersistEngine::load_replica_meta(const ReplicaID& replica_id, ReplicaMetaPayload& payload) const {
    return load_replica_meta(meta_path(replica_id), payload);
}

Status ReplicaMetaPersistEngine::delete_replica_data(const ReplicaID& replica_id) const {
    std::filesystem::path dir = replica_dir(replica_id);
    try {
        if (!std::filesystem::exists(dir)) {
            return Status::OK();
        }
        std::filesystem::remove_all(dir);
        RETURN_IF_INVALID_STATUS(File::fsync_dir(data_dir_))
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::ERROR(fmt::format("delete replica data failed: {}", e.what()));
    }
}

// 返回所有的meta的路径，用来调用load_replica_meta
std::vector<std::filesystem::path> ReplicaMetaPersistEngine::scan_replica_meta_files() const {
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
