#include "storage/persist/wal_store.h"

#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/define.h"
#include "common/framed_record_codec.h"
#include "common/func.h"
#include "common/log.h"
#include "common/metrics/metrics.h"

namespace adviskv::storage {

WalStore::WalStore(std::string wal_path, std::string dir_path)
        : wal_path_(std::move(wal_path)), dir_path_(std::move(dir_path)) {}

Status WalStore::open() { return File::open_for_append(wal_path_, wal_file_); }

Status WalStore::close() { return wal_file_.close(); }

Status WalStore::append(const LogEntry& entry) {
    ADVISKV_METRICS_TIMER("storage_persist_append_wal");
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_request");

    std::unique_lock lock{mutex_};
    Status status = write_wal_to_disk(wal_file_, entry);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_persist_append_wal_failure");
        return status;
    }
    RETURN_IF_INVALID_STATUS(wal_file_.fsync())
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_success");
    return Status::OK();
}

Status WalStore::append_batch(const std::vector<LogEntry>& entries) {
    ADVISKV_METRICS_TIMER("storage_persist_append_wal_batch");
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_request");
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_entry", static_cast<int64_t>(entries.size()));

    std::unique_lock lock{mutex_};
    for (const LogEntry& entry : entries) {
        Status status = write_wal_to_disk(wal_file_, entry);
        if (status.fail()) {
            ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_failure");
            return status;
        }
    }
    RETURN_IF_INVALID_STATUS(wal_file_.fsync())
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_success");
    return Status::OK();
}

Status WalStore::read_batch(std::vector<LogEntry>& entries) const {
    std::unique_lock lock{mutex_};
    return read_wal_batch_unlocked(entries);
}

Status WalStore::rewrite(const std::vector<LogEntry>& entries) {
    ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal");
    std::unique_lock lock{mutex_};
    return rewrite_wal_unlocked(entries);
}

Status WalStore::truncate(const LogIndex& snapshot_index) {
    std::unique_lock lock{mutex_};
    return truncate_wal_unlocked(snapshot_index);
}

Status WalStore::read_wal_batch_unlocked(std::vector<LogEntry>& entries) const {
    WalReadResult result;
    RETURN_IF_INVALID_STATUS(read_wal_from_disk(wal_path_, result))
    entries = std::move(result.entries);
    if (result.error) {
        return Status::ERROR(result.error_msg);
    }
    return Status::OK();
}

Status WalStore::truncate_wal_unlocked(const LogIndex& snapshot_index) {
    std::vector<LogEntry> entries, remain;
    RETURN_IF_INVALID_STATUS(read_wal_batch_unlocked(entries))

    for (LogEntry& entry : entries) {
        if (entry.index <= snapshot_index) continue;
        remain.push_back(std::move(entry));
    }

    return rewrite_wal_unlocked(remain);
}

Status WalStore::rewrite_wal_unlocked(const std::vector<LogEntry>& entries) {
    const std::string tmp_path = wal_path_ + ".tmp";
    File tmp_file;
    RETURN_IF_INVALID_STATUS(File::open_for_write(tmp_path, tmp_file))

    {
        ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal_write");
        for (const LogEntry& entry : entries) {
            RETURN_IF_INVALID_STATUS(write_wal_to_disk(tmp_file, entry))
        }
    }

    {
        ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal_fsync");
        RETURN_IF_INVALID_STATUS(tmp_file.fsync())
    }
    RETURN_IF_INVALID_STATUS(tmp_file.close())

    // 旧主文件先收掉再 rename，失败就整体失败，不留半开状态。
    if (wal_file_.is_open()) {
        RETURN_IF_INVALID_STATUS(wal_file_.close())
    }

    RETURN_IF_INVALID_STATUS(File::rename(tmp_path, wal_path_))
    {
        ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal_dir_fsync");
        RETURN_IF_INVALID_STATUS(File::fsync_dir(dir_path_))
    }

    RETURN_IF_INVALID_STATUS(File::open_for_append(wal_path_, wal_file_))

    return Status::OK();
}

Status WalStore::recover(LogIndex snapshot_index, RecoverResult& result) {
    std::unique_lock lock{mutex_};
    result = {};

    WalReadResult wal_read_result;
    RETURN_IF_INVALID_STATUS(read_wal_from_disk(wal_path_, wal_read_result))

    // 去掉快照覆盖的前缀：快照落盘之后、WAL 截断落盘之前可能崩溃，那磁盘上
    // 的 WAL 就还留着已经被快照覆盖的那一段。
    func::ad_erase_if(wal_read_result.entries,
                      [snapshot_index](const LogEntry& entry) { return entry.index <= snapshot_index; });

    if (!wal_read_result.entries.empty() && wal_read_result.entries.front().index != snapshot_index + 1) {
        // WAL 和快照对不上（中间缺了一段），把快照之后的 WAL 全清掉，
        // 否则下次 recover 还会读到同样的 gap。
        result.entries.clear();
        result.need_recover = true;
        LOG_WARN(
                "wal gap after snapshot during recover, snapshot_index={}, "
                "first_wal_index={}",
                snapshot_index, wal_read_result.entries.front().index);
        RETURN_IF_INVALID_STATUS(rewrite_wal_unlocked(result.entries))
        return Status::OK();
    }

    result.entries = std::move(wal_read_result.entries);

    if (wal_read_result.error) {
        result.need_recover = true;
        LOG_WARN("wal corrupted during recover, reason={}", wal_read_result.error_msg);
        RETURN_IF_INVALID_STATUS(rewrite_wal_unlocked(result.entries))
        return Status::OK();
    }

    // 即使 WAL 本身完整，也可能在 snapshot 发布后、WAL 重写前崩溃。
    // 这里用 snapshot index 重写 WAL，丢弃已经被 snapshot 覆盖的前缀。
    RETURN_IF_INVALID_STATUS(truncate_wal_unlocked(snapshot_index))
    return Status::OK();
}

// buf.len + crc + buf [term + index + op_type + key + value + config fields]
Status WalStore::write_wal_to_disk(const File& file, const LogEntry& entry) {
    return FramedRecord<LogEntry>::encode_to(file, entry);
}

Status WalStore::read_wal_from_disk(const std::string& path, WalReadResult& result) const {
    File file;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(File::open_for_read(path, file, exists))
    if (!exists) {
        result = {};
        return Status::OK();
    }

    result = {};
    LogIndex prev_index{0};
    bool has_prev_index{false};

    while (true) {
        const int64 record_start_offset = result.last_good_offset;

        LogEntry entry;
        size_t consumed_bytes{0};
        Status read_status = FramedRecord<LogEntry>::decode_from(file, entry, consumed_bytes);
        if (read_status.code() == StatusCode::GET_EOF) {
            uint64_t file_size{0};
            if (file.size(file_size).ok() && static_cast<int64>(file_size) > result.last_good_offset) {
                result.error = true;
                result.error_msg = "partial wal record";
            }
            break;
        }
        if (read_status.code() == StatusCode::PARTIAL_READ) {
            result.error = true;
            result.error_msg = "partial wal record";
            break;
        }
        if (read_status.fail()) {
            result.error = true;
            result.error_msg = read_status.msg().empty() ? "wal record decode failure" : read_status.msg();
            break;
        }
        LOG_DEBUG("entry term:{}, index:{}, op_type:{} ", entry.term, entry.index, static_cast<int32>(entry.op_type))
        if (has_prev_index && entry.index != prev_index + 1) {
            result.error = true;
            result.error_msg = fmt::format("wal index is not continuous, prev={}, current={}", prev_index, entry.index);
            break;
        }
        LOG_DEBUG("enry key:{}, value:{}", entry.key, entry.value)
        result.entries.push_back(std::move(entry));
        prev_index = result.entries.back().index;
        has_prev_index = true;
        result.last_good_offset = record_start_offset + consumed_bytes;
    }
    return Status::OK();
}

}  // namespace adviskv::storage
