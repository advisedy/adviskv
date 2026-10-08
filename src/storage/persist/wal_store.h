#pragma once

#include <shared_mutex>
#include <string>
#include <vector>

#include "common/file.h"
#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"

namespace adviskv::storage {

// WAL 的 append / read / rewrite / truncate。
//
// 只被 loop 线程碰（apply 落盘、rewrite、truncate 全在 loop 上），所以那把
// mutex_ 其实是防御性的：同一时刻不会有第二个线程在写同一份 WAL。留着它是
// 为了将来万一有人从别的线程读（比如工具），不至于裸奔。
class WalStore {
public:
    WalStore(std::string wal_path, std::string dir_path);

    Status open();
    Status close();

    Status append(const LogEntry& entry);
    Status append_batch(const std::vector<LogEntry>& entries);
    Status read_batch(std::vector<LogEntry>& entries) const;
    Status rewrite(const std::vector<LogEntry>& entries);
    Status truncate(const LogIndex& snapshot_index);

    // WAL 域内的崩溃恢复：给定"已发布快照的 index"，把 WAL 修成
    // "从 snapshot_index + 1 开始连续"的样子。
    //
    // 为什么这个逻辑归 WalStore 而不是上层：快照落盘、WAL 截断之间可能崩溃，
    // 于是 WAL 里可能残留已经被快照覆盖的前缀，或者干脆和快照对不上有个 gap。
    // 这些都是"给定一个边界，把 WAL 修对"的活，纯粹是 WAL 自己的事；上层只
    // 需要告诉它边界在哪。
    struct RecoverResult {
        std::vector<LogEntry> entries;
        bool need_recover{false};
    };
    Status recover(LogIndex snapshot_index, RecoverResult& result);

private:
    struct WalReadResult {
        std::vector<LogEntry> entries;
        bool error{false};
        int64_t last_good_offset{0};
        std::string error_msg;
    };

    Status write_wal_to_disk(const File& file, const LogEntry& entry);
    Status read_wal_batch_unlocked(std::vector<LogEntry>& entries) const;
    Status read_wal_from_disk(const std::string& path, WalReadResult& result) const;
    Status rewrite_wal_unlocked(const std::vector<LogEntry>& entries);
    Status truncate_wal_unlocked(const LogIndex& snapshot_index);

    std::string wal_path_;
    std::string dir_path_;
    File wal_file_;
    mutable std::shared_mutex mutex_;
};

}  // namespace adviskv::storage
