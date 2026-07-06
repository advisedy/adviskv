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

constexpr size_t kMaxAppendEntriesPerMessage = 256;

}  // namespace

RaftReplication::RaftReplication(const ReplicaID& self_id,
                                 const RaftMembership& membership,
                                 const RaftLog& raft_log, RaftApply& raft_apply)
    : self_id_(self_id),
      membership_(membership),
      raft_log_(raft_log),
      raft_apply_(raft_apply),
      peer_progress_(self_id_, membership_) {}

void RaftReplication::reset_for_leader() {
    peer_progress_.reset_for_leader(membership_, raft_log_.last_log_index());
}

void RaftReplication::broadcast_append_entries(Term current_term,
                                               RaftEffects& effects) {
    for (const RaftMember& member : membership_.raft_members()) {
        if (member.peer.replica_id == self_id_) continue;

        if (peer_progress_.get_next_index(member.peer.replica_id) == 0) {
            peer_progress_.update_next_index(member.peer.replica_id,
                                             raft_log_.last_log_index() + 1);
        }
        LogIndex next_idx = peer_progress_.get_next_index(member.peer.replica_id);

        RaftMessageOr msg =
            build_append_entries_message(member.peer, next_idx, current_term);
        if (msg.has_value()) {
            effects.messages.push_back(std::move(*msg));
        }
    }
}

bool RaftReplication::is_stale_append_response(
    const ReplicaID& replica_id, const AppendEntriesParam& sent_param) const {
    LogIndex sent_next_index = sent_param.prev_log_index + 1;
    return sent_next_index != peer_progress_.get_next_index(replica_id);
}

void RaftReplication::handle_append_ok(const ReplicaID& replica_id,
                                       LogIndex prev_log_index,
                                       size_t entries_size) {
    peer_progress_.handle_append_ok(replica_id, prev_log_index, entries_size);
}

void RaftReplication::handle_append_failed(const ReplicaID& replica_id,
                                           LogIndex follower_last_log_index) {
    peer_progress_.handle_append_failed(replica_id, follower_last_log_index,
                                        raft_log_.last_log_index());
}

RaftReplication::CommitAdvanceResult RaftReplication::try_advance_commit_index(
    Term current_term) {
    CommitAdvanceResult result;
    result.old_commit_index = raft_apply_.commit_index();
    result.new_commit_index = result.old_commit_index;

    for (LogIndex idx = raft_apply_.commit_index() + 1;
         idx <= raft_log_.last_log_index(); ++idx) {
        // TODO 这里将来需要check一下这个term是否需要和当前的term一样吗？
        // 但是好像leader是可以提交上一个leader没有提交的内容吧，所以好像不用

        // 原来如此: leader 确实可以提交前一个 leader 未提交的
        // entry，但不是"直接"提交。Raft 的规则是：只有当当前 term 的 entry
        // 被多数派确认后，commit_index 才会推进，此时之前 term 的 entry 会因为
        // commit_index 的递增而被顺带提交（ trace_commit_log_entries 从
        // last_applied_ 推进到 commit_index_ ，包含了之前 term 的 entry）。
        if (raft_log_.term_at(idx) != current_term) {
            continue;
        }

        int success_cnt = 1;
        for (const auto& member : membership_.voters()) {
            if (member.replica_id == self_id_) {
                continue;
            }
            if (peer_progress_.match_index_at_least(member.replica_id, idx)) {
                success_cnt++;
            }
        }

        if (membership_.has_quorum(success_cnt)) {
            LOG_DEBUG("replica:{} commit_index pushed success. from {} to {}.",
                      self_id_.to_string(), raft_apply_.commit_index(), idx);
            raft_apply_.set_commit_index(idx);
        }
    }

    result.new_commit_index = raft_apply_.commit_index();
    result.advanced = result.new_commit_index > result.old_commit_index;
    return result;
}

void RaftReplication::update_snapshot_watermark(const ReplicaID& replica_id,
                                                LogIndex snapshot_watermark) {
    peer_progress_.update_snapshot_watermark(replica_id, snapshot_watermark);
}

void RaftReplication::clear_snapshot_inflight(const ReplicaID& replica_id,
                                              LogIndex snapshot_index) {
    peer_progress_.clear_snapshot_inflight(replica_id, snapshot_index);
}

LogIndex RaftReplication::next_index(const ReplicaID& replica_id) const {
    return peer_progress_.get_next_index(replica_id);
}

LogIndex RaftReplication::snapshot_watermark(
    const ReplicaID& replica_id) const {
    return peer_progress_.get_snapshot_watermark(replica_id);
}

LogIndex RaftReplication::inflight_snapshot_index(
    const ReplicaID& replica_id) const {
    return peer_progress_.get_inflight_snapshot_index(replica_id);
}

bool RaftReplication::match_index_at_least(
    const ReplicaID& replica_id, LogIndex log_index) const {
    return peer_progress_.match_index_at_least(replica_id, log_index);
}

void RaftReplication::set_next_index_for_test(ReplicaID replica_id,
                                              LogIndex index) {
    peer_progress_.update_next_index(replica_id, index);
}

RaftMessageOr RaftReplication::build_append_entries_message(
    const PeerMember& member, LogIndex next_index, Term current_term) {
    LogIndex prev_log_index = next_index - 1;

    if (prev_log_index < raft_log_.snapshot_index()) {
        // 如果快照已经在发送中了
        LogIndex snapshot_index = raft_log_.snapshot_index();
        if (!peer_progress_.mark_snapshot_inflight(member.replica_id,
                                                   snapshot_index)) {
            LOG_DEBUG(
                "[Raft Replication] replica:{} skip install snapshot to "
                "replica:{}, "
                "inflight_snapshot_index:{}",
                self_id_.to_string(), member.replica_id.to_string(),
                peer_progress_.get_inflight_snapshot_index(member.replica_id));
            return std::nullopt;
        }

        LOG_INFO(
            "[Raft Replication] replica:{} build message, install snapshot to "
            "replica:{}, "
            "snapshot_index:{}",
            self_id_.to_string(), member.replica_id.to_string(),
            snapshot_index);
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
    param.entries =
        raft_log_.entries_from(next_index, kMaxAppendEntriesPerMessage);

    LOG_DEBUG(
        "[Raft Replication] replica:{} build message, append entries to "
        "replica:{}, pre_log_index:{}, prev_log_term:{}, last_log_index:{}, "
        "current_term:{}",
        param.from_replica_id.to_string(), param.to_replica_id.to_string(),
        param.prev_log_index, param.prev_log_term, raft_log_.last_log_index(),
        raft_log_.last_log_term());

    RaftMessage msg;
    msg.type = RaftMessageType::APPEND_ENTRIES;
    msg.target = member;
    msg.append_param = std::move(param);
    return msg;
}

}  // namespace adviskv::storage
