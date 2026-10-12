#include "storage/persist/persist_engine.h"

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "common/path.h"

namespace adviskv::storage {

PersistEngine::PersistEngine(const std::string& data_dir, const ReplicaID& replica_id)
        : data_dir_(data_dir),
          dir_path_(adviskv::replica_dir(data_dir, replica_id).string()),
          replica_id_(replica_id),
          wal_(adviskv::wal_file(dir_path_).string(), dir_path_),
          raft_meta_(adviskv::raft_meta_file(dir_path_).string(), dir_path_),
          snapshot_(adviskv::snapshot_file(dir_path_).string(), dir_path_) {}

PersistEngine::~PersistEngine() {
    Status status = close();
    if (status.fail()) {
        LOG_WARN("[PersistEngine] ~PersistEngine, close failed, status:{}", status.to_string());
    }
}

Status PersistEngine::init() {
    if (data_dir_.empty()) {
        return Status::INVALID_ARGUMENT("persist engine init error: data_dir is epmty");
    }
    if (ReplicaID& id = replica_id_; id.replica_seq == -1 or id.shard_index == -1 or id.table_id == -1) {
        return Status::INVALID_ARGUMENT(
                fmt::format("persist engine init error: replica_id:{} invalid", replica_id_.to_string()));
    }


    try {
        std::filesystem::create_directories(dir_path_);
    } catch (const std::exception& e) {
        LOG_WARN("[PersistEngine] init, persiste create directories failed: {}", dir_path_);
    }

    LOG_DEBUG("[PersistEngine] init, dir_path_={}, wal_path={}, raft_meta_path={}, snapshot_path={}", dir_path_,
              adviskv::wal_file(dir_path_).string(), adviskv::raft_meta_file(dir_path_).string(),
              adviskv::snapshot_file(dir_path_).string());

    RETURN_IF_INVALID_STATUS(wal_.open())
    return Status::OK();
}

Status PersistEngine::close() { return wal_.close(); }

Status PersistEngine::recover(RecoverResult& result) {
    result = {};

    // 快照
    result.snapshot = std::make_shared<SnapshotHeader>();
    RETURN_IF_INVALID_STATUS(snapshot_.load_header(result.snapshot))
    const LogIndex snapshot_index = result.snapshot ? result.snapshot->apply_index : 0;

    // raft_meta
    RETURN_IF_INVALID_STATUS(raft_meta_.load(result.raft_meta))

    WalStore::RecoverResult wal_result;
    RETURN_IF_INVALID_STATUS(wal_.recover(snapshot_index, wal_result))
    result.wal_entries = std::move(wal_result.entries);
    result.need_recover = wal_result.need_recover;

    LOG_DEBUG("persist engine recover ok, snapshot_index:{}, need_recover:{}", snapshot_index, result.need_recover);
    return Status::OK();
}

}  // namespace adviskv::storage