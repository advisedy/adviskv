#include "storage/persist/replica_meta_persist_engine.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "common/binary/codec.h"
#include "common/define.h"
#include "common/file.h"
#include "common/path.h"
#include "storage/persist/persist_codecs.h"

namespace adviskv::storage {

ReplicaMetaPersistEngine::ReplicaMetaPersistEngine(std::string data_dir) : data_dir_(std::move(data_dir)) {}

Status ReplicaMetaPersistEngine::save_replica_meta(const ReplicaMetaPayload& payload) const {
    const std::filesystem::path path =
            adviskv::replica_meta_file(adviskv::replica_dir(data_dir_, payload.init_param.replica_id));

    return File::atomic_replace(path.string(), [&payload](const File& file) {
        FileWriter writer(file);
        RETURN_IF_INVALID_STATUS(binary::encode_crc(payload, writer))
        // 原子替换统一负责 fsync 和发布，这里先把 Writer 的缓冲交给它。
        return writer.flush();
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

    FileReader reader(file);
    return binary::decode_crc(reader, payload);
}

Status ReplicaMetaPersistEngine::load_replica_meta(const ReplicaID& replica_id, ReplicaMetaPayload& payload) const {
    return load_replica_meta(adviskv::replica_meta_file(adviskv::replica_dir(data_dir_, replica_id)), payload);
}

Status ReplicaMetaPersistEngine::delete_replica_data(const ReplicaID& replica_id) const {
    std::filesystem::path dir = adviskv::replica_dir(data_dir_, replica_id);
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
        std::filesystem::path path = adviskv::replica_meta_file(entry.path());
        if (std::filesystem::exists(path)) {
            paths.push_back(std::move(path));
        }
    }
    std::sort(paths.begin(), paths.end());  // 这里sort一下，方便测试啥的。
    return paths;
}

}  // namespace adviskv::storage
