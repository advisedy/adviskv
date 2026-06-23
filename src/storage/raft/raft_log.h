#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "common/type.h"
#include "storage/model/param.h"

namespace adviskv::storage {

// 专门处理raft node 内部的log相关的内容
class RaftLog {
   public:
    struct AppendEntriesResult {
        std::vector<LogEntry> entries_to_append;
        std::optional<std::vector<LogEntry>> entries_to_rewrite;
    };

    struct InstallSnapshotResult {
        std::vector<LogEntry> retained_entries;
    };

    LogIndex snapshot_index() const { return snapshot_index_; }
    Term snapshot_term() const { return snapshot_term_; }
    const std::vector<LogEntry>& entries() const { return log_entries_; }

    LogIndex last_log_index() const;
    Term last_log_term() const;
    int64_t index_to_offset(LogIndex index) const;
    LogIndex offset_to_index(int64_t offset) const;
    Term term_at(LogIndex index) const;

    const LogEntry* entry_at(LogIndex index) const;
    std::vector<LogEntry> entries_from(LogIndex index) const;
    std::vector<LogEntry> entries_from(LogIndex index,
                                       int64 max_count) const;

    LogIndex append_new_entry(Term term, const ProposeParam& param);
    Status append_entries_from_leader(const std::vector<LogEntry>& entries,
                                      AppendEntriesResult& result);

    Status truncate(LogIndex new_snapshot_index);

    InstallSnapshotResult install_snapshot(LogIndex new_snapshot_index,
                                           Term new_snapshot_term);
    std::vector<LogEntry> retained_entries_after_snapshot(
        LogIndex new_snapshot_index, Term new_snapshot_term) const;

    void update_entries(const std::vector<LogEntry>& entries);

   private:
    std::vector<LogEntry> log_entries_;

    LogIndex snapshot_index_{0};
    Term snapshot_term_{0};
};

}  // namespace adviskv::storage