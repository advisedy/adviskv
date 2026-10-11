#include "storage/raft/raft_apply.h"

#include <algorithm>
#include <cassert>

#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/raft/raft_log.h"

namespace adviskv::storage {

RaftApply::RaftApply(const RaftLog& raft_log) : raft_log_(raft_log) {}

void RaftApply::set_commit_index(LogIndex commit_index) { commit_index_ = commit_index; }

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

std::vector<LogEntry> RaftApply::extract_committed_entries(size_t budget_entries, size_t budget_bytes,
                                                           bool& has_more) const {
    std::vector<LogEntry> entries;
    has_more = false;

    assert(last_applied_ >= raft_log_.snapshot_index());

    size_t kv_count = 0;
    size_t kv_bytes = 0;
    bool budget_exhausted = false;
    for (LogIndex i = last_applied_ + 1; i <= commit_index_; ++i) {
        const LogEntry* entry = raft_log_.entry_at(i);
        if (entry == nullptr) {
            LOG_WARN(
                    "entry == nullptr, index:{}, offset:{},  snapshot_index:{}, "
                    "commit_index:{}",
                    i, raft_log_.index_to_offset(i), raft_log_.snapshot_index(), commit_index_);
            continue;
        }

        if (is_config_change_op(entry->op_type)) {
            entries.push_back(*entry);
            continue;
        }

        if (budget_exhausted) {
            has_more = true;
            break;
        }

        if (kv_count >= budget_entries) {
            budget_exhausted = true;
            has_more = true;
            continue;
        }
        const size_t entry_bytes = entry->key.size() + entry->value.size();
        if (kv_count > 0 && budget_bytes > 0 && kv_bytes + entry_bytes > budget_bytes) {
            budget_exhausted = true;
            has_more = true;
            break;
        }
        if (budget_bytes > 0 && kv_count == 0 && entry_bytes > budget_bytes) {
            ADVISKV_METRICS_COUNTER("storage_raft_extract_oversized_entry");
        }
        ++kv_count;
        kv_bytes += entry_bytes;
        entries.push_back(*entry);
    }
    return entries;
}

bool RaftApply::has_committed_current_term_entry(Term current_term) const {
    if (raft_log_.snapshot_index() > 0 && raft_log_.snapshot_index() <= commit_index_ &&
        raft_log_.snapshot_term() == current_term) {
        return true;
    }

    for (LogIndex idx = commit_index_; idx >= raft_log_.snapshot_index() + 1; idx--) {
        if (raft_log_.term_at(idx) == current_term) {
            return true;
        }
    }

    return false;
}

}  // namespace adviskv::storage