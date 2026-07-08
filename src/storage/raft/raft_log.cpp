#include "storage/raft/raft_log.h"

#include <algorithm>
#include <utility>
#include <variant>

#include <fmt/format.h>

#include "common/log.h"
#include "common/model/type.h"
#include "storage/model/param.h"

namespace adviskv::storage {

LogIndex RaftLog::last_log_index() const {
    if (log_entries_.empty()) return snapshot_index_;
    return log_entries_.back().index;
}

Term RaftLog::last_log_term() const {
    if (log_entries_.empty()) return snapshot_term_;
    return log_entries_.back().term;
}

int64_t RaftLog::index_to_offset(LogIndex index) const { return index - snapshot_index_ - 1; }

LogIndex RaftLog::offset_to_index(int64_t offset) const { return offset + 1 + snapshot_index_; }

Term RaftLog::term_at(LogIndex index) const {
    if (index == 0) return 0;
    if (index == snapshot_index_) return snapshot_term_;
    if (index < snapshot_index_) {
        LOG_WARN("raft log get term before snapshot, index={}, snapshot={}", index, snapshot_index_);
        return 0;
    }

    int64_t offset = index_to_offset(index);
    if (offset < 0 || offset >= static_cast<int64_t>(log_entries_.size())) {
        return 0;
    }
    return log_entries_[offset].term;
}

const LogEntry* RaftLog::entry_at(LogIndex index) const {
    int64_t offset = index_to_offset(index);
    if (offset < 0 || offset >= static_cast<int64_t>(log_entries_.size())) {
        LOG_WARN(
                "offset is not valid, offset:{}, log_entries.size:{}, "
                "snapshot_index:{}, last_log_index:{}",
                offset, log_entries_.size(), snapshot_index_, last_log_index());
        return nullptr;
    }
    return &log_entries_[offset];
}

std::vector<LogEntry> RaftLog::entries_from(LogIndex index) const {
    return entries_from(index, last_log_index() - index + 1);
}

std::vector<LogEntry> RaftLog::entries_from(LogIndex index, int64 max_count) const {
    std::vector<LogEntry> entries;
    if (max_count < 0) {
        LOG_WARN("[RaftLog] max count < 0");
        return entries;
    } else if (index > last_log_index() || max_count == 0) {
        return entries;
    }
    int64_t offset = index_to_offset(index);
    if (offset < 0 || offset >= to<int64_t>(log_entries_.size())) {
        LOG_WARN("offset is not valid, offset:{}", offset);
        return {};
    }
    entries.insert(entries.end(), log_entries_.begin() + offset,
                   log_entries_.begin() + std::min(offset + max_count, (int64)log_entries_.size()));
    return entries;
}

LogIndex RaftLog::append_new_entry(Term term, const ProposeParam& param) {
    LogEntry entry;
    entry.term = term;
    entry.index = last_log_index() + 1;

    if (const auto* write = std::get_if<WriteProposal>(&param.payload)) {
        entry.op_type = write->op;
        entry.key = write->key;
        entry.value = write->value;
    } else if (const auto* config = std::get_if<ConfigChangeProposal>(&param.payload)) {
        entry.op_type = config->op;
        entry.config_member = config->member;
        entry.config_replica_id = config->target_replica_id;
    } else {
        entry.op_type = WriteOpType::NONE;
    }

    LogIndex new_index = entry.index;
    log_entries_.push_back(std::move(entry));
    return new_index;
}

Status RaftLog::append_entries_from_leader(const std::vector<LogEntry>& entries, AppendEntriesResult& result) {
    result = {};
    if (entries.empty()) return Status::OK();

    std::vector<LogEntry> updated_entries = log_entries_;
    std::vector<LogEntry> new_entries;
    bool need_rewrite_wal = false;

    auto updated_last_log_index = [&]() -> LogIndex {
        if (updated_entries.empty()) return snapshot_index_;
        return updated_entries.back().index;
    };

    auto updated_get_term = [&](LogIndex index) -> Term {
        if (index == 0) return 0;
        if (index < snapshot_index_) return 0;
        if (index == snapshot_index_) return snapshot_term_;
        int64_t offset = index_to_offset(index);
        if (offset < 0 || offset >= static_cast<int64_t>(updated_entries.size())) {
            return 0;
        }
        return updated_entries[offset].term;
    };

    for (const LogEntry& entry : entries) {
        LogIndex index = entry.index;

        if (index <= snapshot_index_) continue;

        if (index <= updated_last_log_index()) {
            if (updated_get_term(index) != entry.term) {
                updated_entries.resize(index_to_offset(index));
                updated_entries.push_back(entry);
                new_entries.push_back(entry);
                need_rewrite_wal = true;
            }
        } else if (index == updated_last_log_index() + 1) {
            updated_entries.push_back(entry);
            new_entries.push_back(entry);
        } else {
            return Status::ERROR(
                    fmt::format("raft log append entries found gap, last_log_index={}, "
                                "entry_index={}",
                                updated_last_log_index(), index));
        }
    }

    if (!new_entries.empty()) {
        if (need_rewrite_wal) {
            result.entries_to_rewrite = updated_entries;
        } else {
            result.entries_to_append = std::move(new_entries);
        }
    }
    log_entries_ = std::move(updated_entries);
    return Status::OK();
}

// 这个是本地做快照的时候会调用的函数
Status RaftLog::truncate(LogIndex new_snapshot_index) {
    if (new_snapshot_index <= snapshot_index_) {
        LOG_WARN(
                "new_snapshot_index <= snapshot_index_, new_snapshot_index:{}, "
                "snapshot_index:{}",
                new_snapshot_index, snapshot_index_);
        return StatusCode::ERROR;
    }

    if (new_snapshot_index > last_log_index()) {
        LOG_WARN(
                "new_snapshot_index > last_log_index, (from "
                "self)new_snapshot_index:{}, "
                "last_log_index:{}",
                new_snapshot_index, last_log_index());
        return StatusCode::ERROR;
    }

    Term new_snapshot_term = term_at(new_snapshot_index);
    int64 keep_from = index_to_offset(new_snapshot_index + 1);
    log_entries_.erase(log_entries_.begin(), log_entries_.begin() + keep_from);

    snapshot_index_ = new_snapshot_index;
    snapshot_term_ = new_snapshot_term;
    return Status::OK();
}

std::vector<LogEntry> RaftLog::retained_entries_after_snapshot(LogIndex new_snapshot_index,
                                                               Term new_snapshot_term) const {
    if (new_snapshot_index >= last_log_index()) {
        return {};
    }
    if (term_at(new_snapshot_index) != new_snapshot_term) {
        return {};
    }

    int64_t keep_from = index_to_offset(new_snapshot_index + 1);
    if (keep_from < 0 || keep_from > static_cast<int64_t>(log_entries_.size())) {
        LOG_WARN(
                "invalid keep_from when installing snapshot, keep_from:{}, "
                "new_snapshot_index:{}, snapshot_index:{}, log_entries.size:{}",
                keep_from, new_snapshot_index, snapshot_index_, log_entries_.size());
        return {};
    }
    return std::vector<LogEntry>(log_entries_.begin() + keep_from, log_entries_.end());
}

RaftLog::InstallSnapshotResult RaftLog::install_snapshot(LogIndex new_snapshot_index, Term new_snapshot_term) {
    InstallSnapshotResult result;
    result.retained_entries = retained_entries_after_snapshot(new_snapshot_index, new_snapshot_term);

    snapshot_index_ = new_snapshot_index;
    snapshot_term_ = new_snapshot_term;
    log_entries_ = result.retained_entries;

    return result;
}

void RaftLog::update_entries(const std::vector<LogEntry>& entries) { log_entries_ = entries; }

}  // namespace adviskv::storage