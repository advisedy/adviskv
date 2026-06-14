#include "storage/raft/raft_node.h"

#include <fmt/format.h>

#include <algorithm>
#include <cassert>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/model/replica_role.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
namespace adviskv::storage {

static constexpr int32_t HEARTBEAT_INTERVAL = 3;
#define ELECTION_TIMEOUT (func::get_random_int32(15, 30))

RaftNode::RaftNode(const ReplicaID& self_id,
                   const std::vector<PeerMember>& members)
    : self_id_(self_id),
      members_(members),
      election_tick_trigger_(ELECTION_TIMEOUT),
      heartbeat_tick_trigger_(HEARTBEAT_INTERVAL) {
    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;
        if (!match_index_.count(member.replica_id)) {
            match_index_[member.replica_id] = 0;
        }
    }
}

LogIndex RaftNode::last_log_index_unlocked() const {
    if (log_entries_.empty()) return snapshot_index_;
    return log_entries_.back().index;
}

Term RaftNode::last_log_term_unlocked() const {
    if (log_entries_.empty()) return snapshot_term_;
    return log_entries_.back().term;
}

LogIndex RaftNode::last_log_index() const {
    std::lock_guard lock(mutex_);
    return last_log_index_unlocked();
}

Term RaftNode::last_log_term() const {
    std::lock_guard lock(mutex_);
    return last_log_term_unlocked();
}

int RaftNode::quorum_size_unlocked() const {
    return static_cast<int>(members_.size()) / 2 + 1;
}

bool RaftNode::has_quorum_unlocked(int ack_count) const {
    return ack_count >= quorum_size_unlocked();
}

bool RaftNode::later_than_other(Term other_term, LogIndex other_index) const {
    if (last_log_term_unlocked() != other_term) {
        return last_log_term_unlocked() > other_term;
    }
    return last_log_index_unlocked() > other_index;
}

void RaftNode::tick(RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    if (ensure_ready_unlocked().fail()) return;

    if (role_ == ReplicaRole::LEADER) {
        if (heartbeat_tick_trigger_.tick()) {
            broadcast_append_entries(effects);
        }
    } else if (election_tick_trigger_.tick()) {
        become_candidate(effects);
    }

    // if (role_ == ReplicaRole::LEADER) {
    //     heartbeat_ticks_++;
    //     if (heartbeat_ticks_ >= HEARTBEAT_INTERVAL) {
    //         heartbeat_ticks_ = 0;
    //         //
    //         当目前已经是leader的时候，此刻发送如果是日志复制的话，那么pending_message会有内容，否则
    //         broadcast_append_entries();
    //     }
    // } else {
    //     // FOLLOWER 或 CANDIDATE
    //     if (election_ticks_ >= ELECTION_TIMEOUT) {
    //         election_ticks_ = 0;
    //         become_candidate();
    //     }
    // }
}

void RaftNode::become_candidate(RaftEffects& effects) {
    if (ensure_ready_unlocked().fail()) return;
    LOG_INFO("replica:{} start become cadidate", self_id_.to_string());
    election_generation_++;
    current_term_++;
    voted_for_ = self_id_;
    granted_vote_count_ = 1;
    role_ = ReplicaRole::CANDIDATE;
    record_hard_state_unlocked(effects);

    heartbeat_tick_trigger_.stop();
    election_tick_trigger_.reset(ELECTION_TIMEOUT);

    // 如果只有一个节点的话，就直接当选
    if (has_quorum_unlocked(granted_vote_count_)) {
        become_leader(effects);
        return;
    }

    // 给所有 peer 发 RequestVote
    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;
        send_request_vote_to(member, effects);
    }
}

void RaftNode::send_request_vote_to(const PeerMember& member,
                                    RaftEffects& effects) {
    RaftMessage msg;
    msg.type = RaftMessageType::REQUEST_VOTE;
    msg.target = member;
    msg.vote_param.from_replica_id = self_id_;
    msg.vote_param.to_replica_id = member.replica_id;
    msg.vote_param.term = current_term_;
    msg.vote_param.last_log_index = last_log_index_unlocked();
    msg.vote_param.last_log_term = last_log_term_unlocked();
    effects.messages.push_back(std::move(msg));
}

/*

写链路:
在执行写操作之后就加入自己的log_entries，然后广播所有follower，

（flush_message）
然后replica会执行自己产生的message，去实际的发送给别的replica
然后replica接收到了返回来的消息，并且把结果返回给raft_node。
结果返回给raft_node后，raft_node会处理对应的内容:
例如如果是日志复制的回复的话，就可以更新对应的match_idx。

然后replica侧调用apply_committed_entries，去apply到状态机上。同时也会更新raft_node的last_apply_
*/
LogIndex RaftNode::append_new_entry_unlocked(WriteOpType op, const Key& key,
                                             const Value& value,
                                             RaftEffects& effects) {
    LogEntry entry;
    entry.term = current_term_;
    entry.index = last_log_index_unlocked() + 1;
    entry.op_type = op;
    entry.key = key;
    entry.value = value;

    const LogIndex new_index = entry.index;
    log_entries_.push_back(entry);
    effects.entries_to_append.push_back(entry);
    return new_index;
}

std::pair<Status, LogIndex> RaftNode::propose(WriteOpType op, const Key& key,
                                              const Value& value,
                                              RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};

    Status ready_status = ensure_ready_unlocked();
    if (ready_status.fail()) return {ready_status, -1};

    if (role_ != ReplicaRole::LEADER) {
        return {Status{StatusCode::NOT_LEADER, "not leader"}, -1};
    }

    LogIndex new_commit_idx =
        append_new_entry_unlocked(op, key, value, effects);

    broadcast_append_entries(effects);

    if (role_ != ReplicaRole::LEADER) {
        return {Status{StatusCode::NOT_LEADER, "not leader"}, -1};
    }

    if (role_ == ReplicaRole::LEADER and has_quorum_unlocked(1)) {
        try_update_commit_index();
    }

    return {Status::OK(), new_commit_idx};
}

/*
注意，在广播的时候，我们会把每一个follower的 从他们next_idx开始到最新的消息
都发送给他们。 具体是会放到pending_message队列里面。
所以每一次广播之后，我们的pending_message就会更新。
*/
void RaftNode::broadcast_append_entries(RaftEffects& effects) {
    if (ensure_ready_unlocked().fail()) return;
    if (role_ != ReplicaRole::LEADER) return;

    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;

        if (!next_index_.count(member.replica_id)) {
            next_index_[member.replica_id] = last_log_index_unlocked() + 1;
        }

        LogIndex next_idx = next_index_[member.replica_id];
        send_append_entries_to(member, next_idx, effects);
    }
}

void RaftNode::send_append_entries_to(const PeerMember& member,
                                      LogIndex next_index,
                                      RaftEffects& effects) {
    effects.messages.push_back(
        build_append_entries_message_unlocked(member, next_index));
}

RaftMessage RaftNode::build_append_entries_message_unlocked(
    const PeerMember& member, LogIndex next_index) {
    LogIndex prev_log_index = next_index - 1;

    // 如果找不到了话，就发送下载快照的命令。
    if (prev_log_index < snapshot_index_) {
        RaftMessage msg;
        msg.type = RaftMessageType::INSTALL_SNAPSHOT;
        msg.target = member;
        msg.snapshot_param.from_replica_id = self_id_;
        msg.snapshot_param.to_replica_id = member.replica_id;
        msg.snapshot_param.term = current_term_;
        msg.snapshot_param.snapshot_index = snapshot_index_;
        msg.snapshot_param.snapshot_term = snapshot_term_;
        return msg;
    }

    Term prev_log_term = get_term(prev_log_index);

    AppendEntriesParam param;
    param.from_replica_id = self_id_;
    param.to_replica_id = member.replica_id;
    param.term = current_term_;
    param.prev_log_index = prev_log_index;
    param.prev_log_term = prev_log_term;
    param.leader_commit = commit_index_;

    for (LogIndex idx = next_index; idx <= last_log_index_unlocked(); ++idx) {
        param.entries.push_back(log_entries_[index_to_offset(idx)]);
    }

    RaftMessage msg;
    msg.type = RaftMessageType::APPEND_ENTRIES;
    msg.target = member;
    msg.append_param = std::move(param);
    return msg;
}

void RaftNode::handle_request_vote(const RequestVoteParam& param,
                                   RequestVoteResult& result,
                                   RaftEffects& effects) {
    LOG_DEBUG("replica:{} get request vote from {}", self_id_.to_string(),
              param.from_replica_id.to_string());
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    result.term = current_term_;
    result.vote_granted = false;

    // 这边对于recovering状态，我们是会去更新它的current
    // term的，以防止在恢复正常之后可能会接收到一些比较旧的term的节点的消息，并且做出回复
    // if (ensure_not_faulted_unlocked().fail()) {
    //     return;
    // }

    if (param.term < current_term_) {
        return;
    }

    if (param.term > current_term_) {
        // 这里这个函数会把自己的vote_for_给清空掉，会不会导致投了很多次票
        // 但是应该没有问题，毕竟是这种情况应该是发生在多次投票里面，他们的term不一样
        // 既然不一样的话，肯定是最终term最大的那一个去当的leader了，别的会自动变成follower
        become_follower(param.term, effects);
        result.term = current_term_;
    }

    if (ensure_ready_unlocked().fail()) {
        return;
    }

    // 如果比人家的新 （是一定会新，不包括相等）
    if (later_than_other(param.last_log_term, param.last_log_index)) {
        return;
    }

    // 没有投过票，或者投过了，还是这个人
    // 如果投过了这个同一个人的话，其实这里vote_granted是true还是false都无所谓吧，反正对方已经拿到票了。
    // 保障一致性，还是设置成true把

    if (!voted_for_.has_value() ||
        voted_for_.value() == param.from_replica_id) {
        LOG_DEBUG("replica:{} vote to {}, current_term:{}",
                  self_id_.to_string(), param.to_replica_id.table_id,
                  param.to_replica_id.to_string(), current_term_);
        voted_for_ = param.from_replica_id;
        record_hard_state_unlocked(effects);
        result.vote_granted = true;
        election_tick_trigger_.reset(ELECTION_TIMEOUT);
    }
}

// 这个是leadr发过来，raftnode作为follower/cacdidate做的handle
void RaftNode::handle_append_entries(const AppendEntriesParam& param,
                                     AppendEntriesResult& result,
                                     RaftEffects& effects) {
    ADVISKV_METRICS_TIMER("storage_raft_handle_append_entries");
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_request");
    effects = RaftEffects{};
    if (param.entries.empty()) {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_heartbeat");
    } else {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_log");
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_entry",
                                static_cast<int64_t>(param.entries.size()));
    }

    std::lock_guard lock(mutex_);
    result.success = false;
    result.term = current_term_;
    result.last_log_index = last_log_index_unlocked();

    if (param.term < current_term_) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_entries_stale_term");
        return;
    }

    if (param.term > current_term_ || role_ != ReplicaRole::FOLLOWER) {
        // 这里的判断条件是，只要role不是FOLLOWER，就会become，但是如果term和param的term是相等的
        // 然后prev_log_index不一样呢？ 这个时候不需要比较一下index吗？
        // 这里并不需要，毕竟append 不需要去干关于选举方面的事情，
        become_follower(param.term, effects);
    }

    result.term = current_term_;

    if (param.prev_log_index > 0) {
        if (param.prev_log_index < snapshot_index_) {
            LOG_WARN(
                "raft node: follower receive append entries: "
                "param.prev_log_index:{} < snapshot_index_:{}",
                param.prev_log_index, snapshot_index_);
        } else if (get_term(param.prev_log_index) != param.prev_log_term) {
            ADVISKV_METRICS_COUNTER(
                "storage_raft_handle_append_entries_prev_mismatch");
            return;
        }
    }
    // if (param.prev_log_index < snapshot_index_) {
    //     //
    //     压测情况下leader那边高并发发送，follower来不及回复，prev_log_index没更新，follower这边接收到的消息过多导致自己跑了快照，就会触发这种情况
    //     ADVISKV_METRICS_COUNTER(
    //         "storage_raft_handle_append_entries_prev_behind_snapshot");
    //     LOG_WARN(
    //         "raft node: follower receive append entries: "
    //         "param.prev_log_index < snapshot_index_!!");
    //     return;
    // }

    // 把自己的时间reset一下， 选举的
    // election_ticks_ = 0;
    election_tick_trigger_.reset(ELECTION_TIMEOUT);

    if (param.entries.empty()) {
        // 这里是心跳
    } else {
        // 日志复制
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
            if (offset < 0 ||
                offset >= static_cast<int64_t>(updated_entries.size())) {
                return 0;
            }
            return updated_entries[offset].term;
        };

        for (const LogEntry& entry : param.entries) {
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
                LOG_WARN(
                    "storage raft handle append entries found wal gap, "
                    "last_log_index={}, entry_index={}",
                    updated_last_log_index(), index);
                return;
            }
        }

        if (!new_entries.empty()) {
            if (need_rewrite_wal) {
                effects.entries_to_rewrite = updated_entries;
            } else {
                effects.entries_to_append = std::move(new_entries);
            }
        }
        log_entries_ = std::move(updated_entries);
    }

    // 不管是否有entry，也就是不管是日志追加还是心跳，都会需要更新commit_idx
    if (param.leader_commit > commit_index_) {
        commit_index_ =
            std::min(param.leader_commit, last_log_index_unlocked());
    }
    finish_recovering_unlocked();

    result.success = true;
    result.term = current_term_;
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_success");
}

void RaftNode::handle_vote_response(const ReplicaID& from,
                                    const RequestVoteResult& result,
                                    RaftEffects& effects) {
    UNUSED(from);

    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    if (ensure_ready_unlocked().fail()) return;

    // 已经不是 CANDIDATE 了，就直接忽略之前的发起内容
    if (role_ != ReplicaRole::CANDIDATE) return;

    LOG_DEBUG("candidate replica:{} get vote response from replica:{}",
              self_id_.to_string(), from.to_string());

    if (result.term > current_term_) {
        become_follower(result.term, effects);
        return;
    }

    if (!result.vote_granted) return;

    granted_vote_count_++;
    LOG_DEBUG(
        "candidate replica:{} get vote response from replica:{}, self vote "
        "count++ to {}",
        self_id_.to_string(), from.to_string(), granted_vote_count_);

    if (has_quorum_unlocked(granted_vote_count_)) {
        become_leader(effects);
    }
}

// 这里的from是代表的从form那边返回的response。
// 写到一半的时候脑子混了
// 这个是raftnode作为leader，收到了来自别的replica的回应， 自己的handle
// void RaftNode::handle_append_response(const ReplicaID& from,
//                                       const AppendEntriesResult& result) {
//     std::lock_guard lock(mutex_);
//     if (role_ != ReplicaRole::LEADER) return;

//     if (result.term > current_term_) {
//         become_follower(result.term);
//         return;
//     }

//     if (result.success) {
//         // 复制成功，更新 match_index 和 next_index
//         match_index_[from] = last_log_index_unlocked();
//         next_index_[from] = last_log_index_unlocked() + 1;
//         try_update_commit_index();
//     } else {
//         // prev_log 对不上，往前回退
//         auto it = next_index_.find(from);
//         if (it != next_index_.end() && it->second > 1) {
//             --it->second;
//         }
//     }
// }

Status RaftNode::handle_append_response(const ReplicaID& from,
                                        const AppendEntriesParam& sent_param,
                                        const AppendEntriesResult& result,
                                        RaftEffects& effects) {
    ADVISKV_METRICS_TIMER("storage_raft_handle_append_response");
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_request");

    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    RETURN_IF_INVALID_STATUS(ensure_ready_unlocked())

    if (result.term > current_term_) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_response_higher_term");
        become_follower(result.term, effects);
        return Status::NOT_LEADER("higher term");
    }

    if (role_ != ReplicaRole::LEADER) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_response_not_leader");
        return Status::NOT_LEADER();
    }

    if (result.success) {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_success");
        LOG_DEBUG("leader replica:{} append enrties to replica:{} success.",
                  self_id_.to_string(), from.to_string());
        LogIndex matched_index =
            sent_param.prev_log_index + to<LogIndex>(sent_param.entries.size());
        if (matched_index > match_index_[from]) {
            match_index_[from] = matched_index;
        }
        next_index_[from] = match_index_[from] + 1;
        try_update_commit_index();
    } else {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_reject");
        // prev_log 对不上

        // 需要先确认一下关于response的时效性

        if (LogIndex sent_next_index = sent_param.prev_log_index + 1;
            sent_next_index != next_index_[from]) {
            // 说明其实过期了，这个是旧的请求的回应，继续处理到next_index的话可能会影响
            LOG_DEBUG(
                "leader replica:{} sent param.prev_log_index:{} + 1 != "
                "next_index_[from]:{}",
                self_id_.to_string(), sent_param.prev_log_index,
                next_index_[from]);
            return Status::OK();
        }

        LogIndex new_next =
            std::min(result.last_log_index, last_log_index_unlocked()) + 1;

        if (new_next >= next_index_[from] && next_index_[from] > 1) {
            new_next = next_index_[from] - 1;  // 跳转无效，逐次回退
        }
        next_index_[from] = new_next;

        LOG_DEBUG(
            "leader replica:{} append enrties to replica:{} failed. set "
            "next_index:{}",
            self_id_.to_string(), from.to_string(), new_next);
    }
    return Status::OK();
}

std::vector<LogEntry> RaftNode::extract_committed_entries() {
    std::lock_guard lock(mutex_);
    std::vector<LogEntry> entries;
    for (LogIndex i = last_applied_ + 1; i <= commit_index_; i++) {
        entries.push_back(log_entries_[index_to_offset(i)]);
    }
    return entries;
}

void RaftNode::advance_last_applied(LogIndex applied) {
    std::lock_guard lock(mutex_);
    if (applied > last_applied_) {
        last_applied_ = applied;
    }
}

void RaftNode::become_follower(Term later_term, RaftEffects& effects) {
    assert(later_term >= current_term_);

    LOG_INFO("replica:{} become follower, new term:{}", self_id_.to_string(),
             later_term);

    if (later_term > current_term_) {
        current_term_ = later_term;
        voted_for_.reset();
        record_hard_state_unlocked(effects);
    }
    role_ = ReplicaRole::FOLLOWER;
    election_tick_trigger_.reset(ELECTION_TIMEOUT);
}

void RaftNode::become_leader(RaftEffects& effects) {
    if (ensure_ready_unlocked().fail()) return;

    LOG_INFO("replica:{} become leader", self_id_.to_string());
    role_ = ReplicaRole::LEADER;
    // heartbeat_ticks_ = election_ticks_ = 0;
    election_tick_trigger_.stop();
    heartbeat_tick_trigger_.reset(HEARTBEAT_INTERVAL);

    // TODO 为什么当上了leader之后需要把这些全都初始化呢？ 保留原来的值不行吗？
    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;
        next_index_[member.replica_id] = last_log_index_unlocked() + 1;
        match_index_[member.replica_id] = 0;
    }

    append_new_entry_unlocked(
        WriteOpType::NONE, "for debug: this is a no-op entry key",
        "for debug: this is a no-op entry value", effects);
    // 立即广播（含 no-op），相当于心跳 + 日志复制合一
    broadcast_append_entries(effects);

    if (has_quorum_unlocked(1)) {
        try_update_commit_index();
    }
}

// raftnode 作为leader，需要更新自己的commit_idx
// 当收到了follower们关于日志复制的回应时，就调用一下这个函数，去更新自己的commit_idx
void RaftNode::try_update_commit_index() {
    if (ensure_ready_unlocked().fail()) return;

    LogIndex old_commit_index = commit_index_;
    for (LogIndex idx = commit_index_ + 1; idx <= last_log_index_unlocked();
         ++idx) {
        // TODO 这里将来需要check一下这个term是否需要和当前的term一样吗？
        // 但是好像leader是可以提交上一个leader没有提交的内容吧，所以好像不用

        // 原来如此: leader 确实可以提交前一个 leader 未提交的
        // entry，但不是"直接"提交。Raft 的规则是：只有当当前 term 的 entry
        // 被多数派确认后，commit_index 才会推进，此时之前 term 的 entry 会因为
        // commit_index 的递增而被顺带提交（ trace_commit_log_entries 从
        // last_applied_ 推进到 commit_index_ ，包含了之前 term 的 entry）。
        if (log_entries_[index_to_offset(idx)].term != current_term_) {
            continue;
        }

        int success_cnt = 1;
        for (const auto& member : members_) {
            if (member.replica_id == self_id_) {
                continue;
            }
            if (match_index_[member.replica_id] >= idx) success_cnt++;
        }

        if (has_quorum_unlocked(success_cnt)) {
            LOG_DEBUG("replica:{} commit_index pushed success. from {} to {}.",
                      self_id_.to_string(), commit_index_, idx);
            commit_index_ = idx;
        }
    }
    if (commit_index_ > old_commit_index) {
        ADVISKV_METRICS_COUNTER("storage_raft_commit_index_advance");
        ADVISKV_METRICS_COUNTER(
            "storage_raft_committed_entry",
            static_cast<int64_t>(commit_index_ - old_commit_index));
    }
}

int64_t RaftNode::index_to_offset(LogIndex index) const {
    return index - snapshot_index_ - 1;
}
LogIndex RaftNode::offset_to_index(int64_t offset) const {
    return offset + 1 + snapshot_index_;
}

Term RaftNode::get_term(LogIndex index) const {
    if (index == 0) return 0;
    if (index == snapshot_index_) return snapshot_term_;
    if (index < snapshot_index_) {
        //
        LOG_WARN("... get term");
        return 0;
    }
    int64_t offset = index_to_offset(index);
    if (offset < 0 || offset >= (int64_t)(log_entries_.size())) {
        return 0;
    }
    return log_entries_[offset].term;
}

Status RaftNode::ensure_ready_unlocked() const {
    if (state_ == RaftNodeState::RECOVERING) {
        return Status::IS_RECOVERING("raft node is recovering");
    }
    if (state_ != RaftNodeState::READY) {
        return Status::NOT_INIT("raft node is not ready");
    }
    return Status::OK();
}

void RaftNode::record_hard_state_unlocked(RaftEffects& effects) const {
    effects.hard_state = RaftMeta{current_term_, voted_for_};
}

// void try_take_snapshot();
// void RaftNode::()

Status RaftNode::truncate_log(LogIndex new_snapshot_index) {
    std::lock_guard lock(mutex_);
    if (new_snapshot_index <= snapshot_index_ or
        new_snapshot_index > last_applied_) {
        return StatusCode::ERROR;
    }
    // new_snapshot_index should <= last_applied_

    Term new_snapshot_term = get_term(new_snapshot_index);

    int64_t keep_from = index_to_offset(new_snapshot_index + 1);

    log_entries_.erase(log_entries_.begin(), log_entries_.begin() + keep_from);

    snapshot_index_ = new_snapshot_index;
    snapshot_term_ = new_snapshot_term;

    return Status::OK();
}

void RaftNode::install_snapshot_unlocked(LogIndex new_snapshot_index,
                                         Term new_snapshot_term) {
    snapshot_index_ = new_snapshot_index;
    snapshot_term_ = new_snapshot_term;
    log_entries_.clear();
    if (commit_index_ < snapshot_index_) {
        commit_index_ = snapshot_index_;
    }
    if (last_applied_ < snapshot_index_) {
        last_applied_ = snapshot_index_;
    }

    finish_recovering_unlocked();
}

void RaftNode::install_local_snapshot(LogIndex new_snapshot_index,
                                      Term new_snapshot_term) {
    std::lock_guard lock(mutex_);
    install_snapshot_unlocked(new_snapshot_index, new_snapshot_term);
}

Status RaftNode::install_leader_snapshot(LogIndex new_snapshot_index,
                                         Term new_snapshot_term,
                                         Term leader_term,
                                         RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};

    if (leader_term < current_term_) {
        return Status::ERROR(
            fmt::format("replica_id:{} install_leader_snapshot: leader term:{} "
                        "< current term:{}",
                        self_id_.to_string(), leader_term, current_term_));
    }

    if (leader_term > current_term_ || role_ != ReplicaRole::FOLLOWER) {
        become_follower(leader_term, effects);
    } else {
        election_tick_trigger_.reset(ELECTION_TIMEOUT);
    }

    install_snapshot_unlocked(new_snapshot_index, new_snapshot_term);
    return Status::OK();
}

void RaftNode::handle_install_snapshot_response(
    const ReplicaID& from, const InstallSnapshotResult& result,
    RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    if (ensure_ready_unlocked().fail()) return;
    if (role_ != ReplicaRole::LEADER) return;

    if (result.term > current_term_) {
        become_follower(result.term, effects);
        return;
    }

    if (!result.success) return;

    next_index_[from] = snapshot_index_ + 1;
    match_index_[from] = snapshot_index_;
}

Status RaftNode::prepare_install_snapshot(Term leader_term,
                                          LogIndex new_snapshot_index,
                                          RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};

    if (leader_term < current_term_) {  // 发送过来的leader的term低
        return Status::ERROR(
            fmt::format("leade term:{} is oldear than current term:{}",
                        leader_term, current_term_));
    }

    if (leader_term > current_term_ || role_ != ReplicaRole::FOLLOWER) {
        become_follower(leader_term, effects);
    }

    if (new_snapshot_index <= snapshot_index_) {
        return Status::ERROR(fmt::format(
            "leader snapshot_index:{} is older than current snapshot_index:{}",
            new_snapshot_index, snapshot_index_));
    }

    return Status::OK();
}

void RaftNode::update_raft_meta(const RaftMeta& meta) {
    std::lock_guard lock(mutex_);
    current_term_ = meta.current_term;
    voted_for_ = meta.voted_for;
}

void RaftNode::update_log_entries(const std::vector<LogEntry>& entries) {
    std::lock_guard lock(mutex_);
    log_entries_ = entries;
}

void RaftNode::enter_recovering() {
    std::lock_guard lock(mutex_);
    state_ = RaftNodeState::RECOVERING;
    role_ = ReplicaRole::FOLLOWER;
    election_tick_trigger_.stop();
    heartbeat_tick_trigger_.stop();
}

void RaftNode::finish_recovering_unlocked() {
    if (state_ != RaftNodeState::RECOVERING) return;

    state_ = RaftNodeState::READY;
    election_tick_trigger_.reset(ELECTION_TIMEOUT);
}

// 判判断当前这个term，这个leader是否已经提交过entry了
// 原因的话是因为如果他没有提交过的话，那这个commit
// index就还没有这个及时的更新到，那么就有可能会导致客户端最终那边会读到旧的数据。
bool RaftNode::has_committed_current_term_entry_unlocked() const {
    if (snapshot_index_ > 0 && snapshot_index_ <= commit_index_ &&
        snapshot_term_ == current_term_) {
        return true;
    }

    for (LogIndex idx = commit_index_; idx >= snapshot_index_ + 1; idx--) {
        if (get_term(idx) == current_term_) {
            return true;
        }
    }

    return false;
}

// 其实由于我们get操作会专门发送一次，所以导致同样的append_entires
// 可能会多次发送
// ，但是无伤大雅，handle_append_entries里面会判断出来new_entries是空的
Status RaftNode::build_append_entries_for_read(RaftEffects& effects,
                                               LogIndex& read_index,
                                               Term& read_term) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    RETURN_IF_INVALID_STATUS(ensure_ready_unlocked())

    if (role_ != ReplicaRole::LEADER) return Status::NOT_LEADER("not leader");
    if (!has_committed_current_term_entry_unlocked()) {
        return Status::NOT_YET_COMMIT("current term entry is not committed");
    }

    read_term = current_term_;
    read_index = commit_index_;

    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;

        if (!next_index_.count(member.replica_id)) {
            next_index_[member.replica_id] = last_log_index_unlocked() + 1;
        }

        LogIndex next_idx = next_index_[member.replica_id];

        effects.messages.push_back(
            build_append_entries_message_unlocked(member, next_idx));
    }
    return Status::OK();
}

#undef ELECTION_TIMEOUT

}  // namespace adviskv::storage
