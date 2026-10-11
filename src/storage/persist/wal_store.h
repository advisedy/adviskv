#pragma once

#include <shared_mutex>
#include <string>
#include <vector>

#include "common/file.h"
#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"

namespace adviskv::storage {

class WalStore {
public:
    WalStore(std::string wal_path, std::string dir_path);

    Status open();
    Status close();

    Status append(const LogEntry& entry);
    Status append_batch(const std::vector<LogEntry>& entries);
    Status read_batch(std::vector<LogEntry>& entries) const;
    Status rewrite(const std::vector<LogEntry>& entries);

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

    std::string wal_path_;
    std::string dir_path_;
    File wal_file_;
    mutable std::shared_mutex mutex_;
};

}  // namespace adviskv::storage
