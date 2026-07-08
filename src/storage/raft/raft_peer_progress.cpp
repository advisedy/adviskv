#include "storage/raft/raft_peer_progress.h"

#include <algorithm>

#include "common/log.h"

namespace adviskv::storage {

RaftPeerProgress::RaftPeerProgress(ReplicaID replica_id, const RaftMembership& membership) : self_id_(replica_id) {
    for (const PeerMember& member : membership.peer_members()) {
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

LogIndex RaftPeerProgress::get_next_index(ReplicaID replica_id) const {
    auto it = next_index_.find(replica_id);
    if (it == next_index_.end()) {
        return 0;
    }
    return it->second;
}

void RaftPeerProgress::update_next_index(ReplicaID replica_id, LogIndex log_index) {
    next_index_[replica_id] = log_index;
}

LogIndex RaftPeerProgress::get_match_index(ReplicaID replica_id) const {
    auto it = match_index_.find(replica_id);
    if (it == match_index_.end()) {
        return 0;
    }
    return it->second;
}

void RaftPeerProgress::update_match_index(ReplicaID replica_id, LogIndex log_index) {
    match_index_[replica_id] = log_index;
}

void RaftPeerProgress::reset_for_leader(const RaftMembership& membership, LogIndex last_log_index) {
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

void RaftPeerProgress::update_snapshot_watermark(ReplicaID replica_id, LogIndex snapshot_watermark) {
    {  // log
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

LogIndex RaftPeerProgress::get_snapshot_watermark(ReplicaID replica_id) const {
    if (auto it = snapshot_watermark_.find(replica_id); it != snapshot_watermark_.end()) {
        return it->second;
    }
    return 0;
}

LogIndex RaftPeerProgress::get_inflight_snapshot_index(ReplicaID replica_id) const {
    auto it = inflight_snapshot_index_.find(replica_id);
    if (it == inflight_snapshot_index_.end()) {
        return 0;
    }
    return it->second;
}

bool RaftPeerProgress::mark_snapshot_inflight(ReplicaID replica_id, LogIndex snapshot_index) {
    if (get_inflight_snapshot_index(replica_id) > 0) {
        return false;
    }
    inflight_snapshot_index_[replica_id] = snapshot_index;
    return true;
}

void RaftPeerProgress::clear_snapshot_inflight(ReplicaID replica_id, LogIndex snapshot_index) {
    auto it = inflight_snapshot_index_.find(replica_id);
    if (it == inflight_snapshot_index_.end()) {
        return;
    }
    if (it->second == snapshot_index) {
        it->second = 0;
    }
}

void RaftPeerProgress::handle_append_ok(ReplicaID replica_id, LogIndex prev_log_index, size_t entries_size) {
    LogIndex matched_index = prev_log_index + static_cast<LogIndex>(entries_size);
    if (matched_index > get_match_index(replica_id)) {
        update_match_index(replica_id, matched_index);
    }
    update_next_index(replica_id, get_match_index(replica_id) + 1);
}

void RaftPeerProgress::handle_append_failed(ReplicaID replica_id, LogIndex follower_last_log_index,
                                            LogIndex leader_last_log_index) {
    LogIndex current_next = get_next_index(replica_id);
    LogIndex new_next = std::min(follower_last_log_index, leader_last_log_index) + 1;

    if (new_next >= current_next && current_next > 1) {
        new_next = current_next - 1;
    }

    new_next = std::max(new_next, get_match_index(replica_id) + 1);
    update_next_index(replica_id, new_next);
}

bool RaftPeerProgress::match_index_at_least(ReplicaID replica_id, LogIndex log_index) const {
    return get_match_index(replica_id) >= log_index;
}

}  // namespace adviskv::storage