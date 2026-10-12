#pragma once

#include <filesystem>
#include <string>

#include "common/model/model.h"

namespace adviskv {

// Meta 默认目录也放在路径入口；配置文件指定目录时还是以配置为准。
inline constexpr char K_DEFAULT_META_DATA_DIR[] = "build/demo/data/meta";

// 路径规则都从这里拿。配置加载只负责传入目录，写入、恢复和删除别各拼一份。
inline std::filesystem::path project_root_dir() { return std::filesystem::path(ADVISKV_PROJECT_ROOT); }

// 相对路径仍然从项目根目录出发，绝对路径只做规范化，不改变现有配置的含义。
inline std::filesystem::path path_from_project_root(const std::filesystem::path& path) {
    if (path.empty()) {
        return project_root_dir();
    }
    if (path.is_absolute()) {
        return path.lexically_normal();
    }
    return (project_root_dir() / path).lexically_normal();
}

// Meta 的持久化文件。
inline std::filesystem::path meta_file(const std::filesystem::path& data_dir) {
    return data_dir / "meta.json";
}

// Storage 的副本目录和正式文件，保持现有磁盘布局不变。
inline std::filesystem::path replica_dir(const std::filesystem::path& data_dir, const ReplicaID& replica_id) {
    return data_dir / (std::to_string(replica_id.table_id) + "-" + std::to_string(replica_id.shard_index));
}

inline std::filesystem::path wal_file(const std::filesystem::path& dir) { return dir / "wal.log"; }

inline std::filesystem::path raft_meta_file(const std::filesystem::path& dir) { return dir / "raft_meta"; }

inline std::filesystem::path snapshot_file(const std::filesystem::path& dir) { return dir / "snapshot"; }

inline std::filesystem::path replica_meta_file(const std::filesystem::path& dir) { return dir / "replica_meta"; }

// 快照接收和本地生成各有自己的临时文件，别和通用原子替换的临时文件混用。
inline std::filesystem::path receiving_snapshot_file(const std::filesystem::path& dir, LogIndex snapshot_index) {
    return dir / ("snapshot.receiving." + std::to_string(snapshot_index) + ".tmp");
}

inline std::filesystem::path candidate_snapshot_file(const std::filesystem::path& dir, LogIndex capture_index) {
    return dir / ("snapshot." + std::to_string(capture_index) + ".tmp");
}

// 原子替换的临时文件和目标放在同一个目录，后面才能直接 rename。
inline std::filesystem::path atomic_replace_temp_path(const std::filesystem::path& target) {
    auto tmp = target;
    tmp += ".tmp";
    return tmp;
}

// 日志输出位置由配置决定，目录和文件名的组合也走同一个入口。
inline std::filesystem::path log_file(const std::filesystem::path& log_dir,
                                     const std::filesystem::path& filename) {
    return log_dir / filename;
}

}  // namespace adviskv
