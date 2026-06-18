#include "storage/raft/raft_apply.h"

#include <algorithm>
#include <cassert>

#include "common/log.h"
#include "storage/raft/raft_log.h"

namespace adviskv::storage {

RaftApply::RaftApply(const RaftLog& raft_log) : raft_log_(raft_log) {}

void RaftApply::set_commit_index(LogIndex commit_index) {
    commit_index_ = commit_index;
}

void RaftApply::advance_commit_index_from_leader(LogIndex leader_commit) {
    if (leader_commit > commit_index_) {
        commit_index_ = std::min(leader_commit, raft_log_.last_log_index());
    }
}

void RaftApply::advance_last_applied(LogIndex applied) {
    if (applied > last_applied_) {
        last_applied_ = applied;
    }
}

void RaftApply::install_snapshot(LogIndex snapshot_index) {
    if (commit_index_ < snapshot_index) {
        commit_index_ = snapshot_index;
    }
    if (last_applied_ < snapshot_index) {
        last_applied_ = snapshot_index;
    }
}

// 提取出来已经commit，但是还没有apply的entry
std::vector<LogEntry> RaftApply::extract_committed_entries() const {
    std::vector<LogEntry> entries;
    
    assert(last_applied_ >= raft_log_.snapshot_index());

    for (LogIndex i = last_applied_ + 1; i <= commit_index_; ++i) {
        const LogEntry* entry = raft_log_.entry_at(i);
        if (entry == nullptr) {
            LOG_WARN(
                "entry == nullptr, index:{}, offset:{},  snapshot_index:{}, "
                "commit_index:{}",
                i, raft_log_.index_to_offset(i), raft_log_.snapshot_index(),
                commit_index_);
            continue;
        }
        LOG_DEBUG(
            "entry, index:{}, offset:{} entry:[{}], snapshot_index:{}, "
            "commit_index:{}",
            i, raft_log_.index_to_offset(i), entry->to_string(),
            raft_log_.snapshot_index(), commit_index_);
        entries.push_back(*entry);
    }
    return entries;
}

bool RaftApply::has_committed_current_term_entry(Term current_term) const {
    if (raft_log_.snapshot_index() > 0 &&
        raft_log_.snapshot_index() <= commit_index_ &&
        raft_log_.snapshot_term() == current_term) {
        return true;
    }

    for (LogIndex idx = commit_index_; idx >= raft_log_.snapshot_index() + 1;
         idx--) {
        if (raft_log_.term_at(idx) == current_term) {
            return true;
        }
    }

    return false;
}

}  // namespace adviskv::storage