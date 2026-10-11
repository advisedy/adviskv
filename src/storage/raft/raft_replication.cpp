#include "storage/raft/raft_replication.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/model/model.h"
#include "storage/raft/raft_log.h"

namespace adviskv::storage {

namespace {

constexpr size_t K_MAX_APPEND_ENTRIES_PER_MESSAGE = 256;

}  // namespace

RaftReplication::RaftReplication(const ReplicaID& self_id, const RaftMembership& membership, const RaftLog& raft_log,
                                 RaftApply& raft_apply)
        : self_id_(self_id), membership_(membership), raft_log_(raft_log), raft_apply_(raft_apply) {
    for (const PeerMember& member : membership_.peer_members()) {
        if (member.replica_id == self_id_) continue;
        if (!match_index_.count(member.replica_id)) {
            match_index_[member.replica_id] = 0;
        }
        if (!snapshot_watermark_.count(member.replica_id)) {
            snapshot_watermark_[member.replica_id] = 0;
        }
        if (!inflight_snapshot_index_.count(member.replica_id)) {
            inflight_snapshot_index_[member.replica_id] = 0;
        }
    }
}


LogIndex RaftReplication::get_next_index(ReplicaID replica_id) const {
    auto it = next_index_.find(replica_id);
    if (it == next_index_.end()) {
        return 0;
    }
    return it->second;
}

void RaftReplication::update_next_index(ReplicaID replica_id, LogIndex log_index) {
    next_index_[replica_id] = log_index;
}

LogIndex RaftReplication::get_match_index(ReplicaID replica_id) const {
    auto it = match_index_.find(replica_id);
    if (it == match_index_.end()) {
        return 0;
    }
    return it->second;
}

void RaftReplication::update_match_index(ReplicaID replica_id, LogIndex log_index) {
    match_index_[replica_id] = log_index;
}

void RaftReplication::reset_peer_progress_for_leader(const RaftMembership& membership, LogIndex last_log_index) {
    next_index_.clear();
    match_index_.clear();
    snapshot_watermark_.clear();
    inflight_snapshot_index_.clear();
    for (const PeerMember& member : membership.peer_members()) {
        if (member.replica_id == self_id_) continue;
        next_index_[member.replica_id] = last_log_index + 1;
        match_index_[member.replica_id] = 0;
        snapshot_watermark_[member.replica_id] = 0;
        inflight_snapshot_index_[member.replica_id] = 0;
    }
}

void RaftReplication::update_snapshot_watermark(const ReplicaID& replica_id, LogIndex snapshot_watermark) {
    {
        LogIndex old_watermark = get_snapshot_watermark(replica_id);
        LogIndex old_match = get_match_index(replica_id);
        LogIndex old_next = get_next_index(replica_id);

        LogIndex new_watermark = std::max(old_watermark, snapshot_watermark);
        LogIndex new_match = std::max(old_match, snapshot_watermark);
        LogIndex new_next = std::max(old_next, snapshot_watermark + 1);

        LOG_DEBUG(
                "[Raft PeerProgress] update snapshot watermark, replica_id:{}, "
                "snapshot_watermark:[old:{}, new:{}], match_index:[old:{}, "
                "new:{}], "
                "next_index:[old:{}, new:{}]",
                replica_id.to_string(), old_watermark, new_watermark, old_match, new_match, old_next, new_next);
    }

    snapshot_watermark_[replica_id] = std::max(get_snapshot_watermark(replica_id), snapshot_watermark);
    match_index_[replica_id] = std::max(get_match_index(replica_id), snapshot_watermark);
    next_index_[replica_id] = std::max(get_next_index(replica_id), snapshot_watermark + 1);
}

LogIndex RaftReplication::get_snapshot_watermark(ReplicaID replica_id) const {
    if (auto it = snapshot_watermark_.find(replica_id); it != snapshot_watermark_.end()) {
        return it->second;
    }
    return 0;
}

LogIndex RaftReplication::get_inflight_snapshot_index(ReplicaID replica_id) const {
    auto it = inflight_snapshot_index_.find(replica_id);
    if (it == inflight_snapshot_index_.end()) {
        return 0;
    }
    return it->second;
}

bool RaftReplication::mark_snapshot_inflight(ReplicaID replica_id, LogIndex snapshot_index) {
    if (get_inflight_snapshot_index(replica_id) > 0) {
        return false;
    }
    inflight_snapshot_index_[replica_id] = snapshot_index;
    return true;
}

void RaftReplication::handle_peer_append_ok(ReplicaID replica_id, LogIndex prev_log_index, size_t entries_size) {
    LogIndex matched_index = prev_log_index + static_cast<LogIndex>(entries_size);
    if (matched_index > get_match_index(replica_id)) {
        update_match_index(replica_id, matched_index);
    }
    update_next_index(replica_id, get_match_index(replica_id) + 1);
}

void RaftReplication::handle_peer_append_failed(ReplicaID replica_id, LogIndex follower_last_log_index,
                                                LogIndex leader_last_log_index) {
    LogIndex current_next = get_next_index(replica_id);
    LogIndex new_next = std::min(follower_last_log_index, leader_last_log_index) + 1;

    if (new_next >= current_next && current_next > 1) {
        new_next = current_next - 1;
    }

    new_next = std::max(new_next, get_match_index(replica_id) + 1);
    update_next_index(replica_id, new_next);
}


void RaftReplication::reset_for_leader() {
    reset_peer_progress_for_leader(membership_, raft_log_.last_log_index());
}

void RaftReplication::broadcast_append_entries(Term current_term, RaftEffects& effects) {
    for (const RaftMember& member : membership_.raft_members()) {
        if (member.peer.replica_id == self_id_) continue;

        if (get_next_index(member.peer.replica_id) == 0) {
            update_next_index(member.peer.replica_id, raft_log_.last_log_index() + 1);
        }
        LogIndex next_idx = get_next_index(member.peer.replica_id);

        RaftMessageOr msg = build_append_entries_message(member.peer, next_idx, current_term);
        if (msg.has_value()) {
            effects.messages.push_back(std::move(*msg));
        }
    }
}

bool RaftReplication::is_stale_append_response(const ReplicaID& replica_id,
                                               const AppendEntriesParam& sent_param) const {
    LogIndex sent_next_index = sent_param.prev_log_index + 1;
    return sent_next_index != get_next_index(replica_id);
}

void RaftReplication::handle_append_ok(const ReplicaID& replica_id, LogIndex prev_log_index, size_t entries_size) {
    handle_peer_append_ok(replica_id, prev_log_index, entries_size);
}

void RaftReplication::handle_append_failed(const ReplicaID& replica_id, LogIndex follower_last_log_index) {
    handle_peer_append_failed(replica_id, follower_last_log_index, raft_log_.last_log_index());
}

RaftReplication::CommitAdvanceResult RaftReplication::try_advance_commit_index(Term current_term) {
    CommitAdvanceResult result;
    result.old_commit_index = raft_apply_.commit_index();
    result.new_commit_index = result.old_commit_index;

    for (LogIndex idx = raft_apply_.commit_index() + 1; idx <= raft_log_.last_log_index(); ++idx) {

        if (raft_log_.term_at(idx) != current_term) {
            continue;
        }

        int success_cnt = 1;
        for (const auto& member : membership_.voters()) {
            if (member.replica_id == self_id_) {
                continue;
            }
            if (match_index_at_least(member.replica_id, idx)) {
                success_cnt++;
            }
        }

        if (membership_.has_quorum(success_cnt)) {
            LOG_DEBUG("replica:{} commit_index pushed success. from {} to {}.", self_id_.to_string(),
                      raft_apply_.commit_index(), idx);
            raft_apply_.set_commit_index(idx);
        }
    }

    result.new_commit_index = raft_apply_.commit_index();
    result.advanced = result.new_commit_index > result.old_commit_index;
    return result;
}

void RaftReplication::clear_snapshot_inflight(const ReplicaID& replica_id, LogIndex snapshot_index) {
    auto it = inflight_snapshot_index_.find(replica_id);
    if (it == inflight_snapshot_index_.end()) {
        return;
    }
    if (it->second == snapshot_index) {
        it->second = 0;
    }
}

LogIndex RaftReplication::next_index(const ReplicaID& replica_id) const { return get_next_index(replica_id); }

LogIndex RaftReplication::snapshot_watermark(const ReplicaID& replica_id) const {
    return get_snapshot_watermark(replica_id);
}

LogIndex RaftReplication::inflight_snapshot_index(const ReplicaID& replica_id) const {
    return get_inflight_snapshot_index(replica_id);
}

bool RaftReplication::match_index_at_least(const ReplicaID& replica_id, LogIndex log_index) const {
    return get_match_index(replica_id) >= log_index;
}

void RaftReplication::set_next_index_for_test(ReplicaID replica_id, LogIndex index) {
    update_next_index(replica_id, index);
}

RaftMessageOr RaftReplication::build_append_entries_message(const PeerMember& member, LogIndex next_index,
                                                            Term current_term) {
    LogIndex prev_log_index = next_index - 1;

    if (prev_log_index < raft_log_.snapshot_index()) {
        LogIndex snapshot_index = raft_log_.snapshot_index();
        if (!mark_snapshot_inflight(member.replica_id, snapshot_index)) {
            LOG_DEBUG(
                    "[Raft Replication] replica:{} skip install snapshot to "
                    "replica:{}, "
                    "inflight_snapshot_index:{}",
                    self_id_.to_string(), member.replica_id.to_string(),
                    get_inflight_snapshot_index(member.replica_id));
            return std::nullopt;
        }

        LOG_INFO(
                "[Raft Replication] replica:{} build message, install snapshot to "
                "replica:{}, "
                "snapshot_index:{}",
                self_id_.to_string(), member.replica_id.to_string(), snapshot_index);
        RaftMessage msg;
        msg.type = RaftMessageType::INSTALL_SNAPSHOT;
        msg.target = member;
        msg.snapshot_param.from_replica_id = self_id_;
        msg.snapshot_param.to_replica_id = member.replica_id;
        msg.snapshot_param.term = current_term;
        msg.snapshot_param.snapshot_index = snapshot_index;
        msg.snapshot_param.snapshot_term = raft_log_.snapshot_term();
        return msg;
    }

    AppendEntriesParam param;
    param.from_replica_id = self_id_;
    param.to_replica_id = member.replica_id;
    param.term = current_term;
    param.prev_log_index = prev_log_index;
    param.prev_log_term = raft_log_.term_at(prev_log_index);
    param.leader_commit = raft_apply_.commit_index();
    param.entries = raft_log_.entries_from(next_index, K_MAX_APPEND_ENTRIES_PER_MESSAGE);

    LOG_DEBUG(
            "[Raft Replication] replica:{} build message, append entries to "
            "replica:{}, pre_log_index:{}, prev_log_term:{}, last_log_index:{}, "
            "current_term:{}",
            param.from_replica_id.to_string(), param.to_replica_id.to_string(), param.prev_log_index,
            param.prev_log_term, raft_log_.last_log_index(), raft_log_.last_log_term());

    RaftMessage msg;
    msg.type = RaftMessageType::APPEND_ENTRIES;
    msg.target = member;
    msg.append_param = std::move(param);
    return msg;
}

}  // namespace adviskv::storage
