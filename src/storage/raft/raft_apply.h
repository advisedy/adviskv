#pragma once

#include <vector>

#include "common/model/type.h"
#include "storage/model/param.h"

namespace adviskv::storage {

class RaftLog;

// 负责记录已经 commit / apply 到哪里的进度。
// 和名字不同，不仅会负责apply的推进，还有commit的推进
class RaftApply {
   public:
    explicit RaftApply(const RaftLog& raft_log);

    LogIndex commit_index() const { return commit_index_; }
    LogIndex last_applied() const { return last_applied_; }

    void set_commit_index(LogIndex commit_index);
    void advance_commit_index_from_leader(LogIndex leader_commit);
    void advance_last_applied(LogIndex applied);
    void install_snapshot(LogIndex snapshot_index);

    std::vector<LogEntry> extract_committed_entries() const;
    bool has_committed_current_term_entry(Term current_term) const;

   private:
    const RaftLog& raft_log_;
    LogIndex commit_index_{0};
    LogIndex last_applied_{0};
};

}  // namespace adviskv::storage