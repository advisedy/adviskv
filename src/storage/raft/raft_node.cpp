#include "storage/raft/raft_node.h"

#include <algorithm>
#include <cassert>
#include <random>
#include <utility>

#include "common/func.h"
#include "common/log.h"
#include "common/type.h"
#include "storage/model/param.h"

namespace adviskv::storage {

static constexpr int32_t HEARTBEAT_INTERVAL = 3;
#define ELECTION_TIMEOUT (get_random_int32(15, 30))

RaftNode::RaftNode(const ReplicaID& self_id,
                   const std::vector<PeerMember>& members)
    : self_id_(self_id),
      members_(members),
      election_tick_trigger_(ELECTION_TIMEOUT,
                             [this]() { become_candidate(); }),
      heartbeat_tick_trigger_(HEARTBEAT_INTERVAL,
                              [this]() { broadcast_append_entries(); }) {}

LogIndex RaftNode::last_log_index() const {
    if (log_entries_.empty()) return 0;
    return log_entries_.back().index;
}

Term RaftNode::last_log_term() const {
    if (log_entries_.empty()) return 0;
    return log_entries_.back().term;
}

bool RaftNode::later_than_other(Term other_term, LogIndex other_index) const {
    if (last_log_term() != other_term) {
        return last_log_term() > other_term;
    }
    return last_log_index() > other_index;
}

void RaftNode::tick() {
    // election_ticks_++;
    if (role_ == ReplicaRole::LEADER) {
        heartbeat_tick_trigger_.tick();
    } else {
        election_tick_trigger_.tick();
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

void RaftNode::become_candidate() {
    election_generation_++;
    current_term_++;
    voted_for_ = self_id_;
    granted_vote_count_ = 1;
    role_ = ReplicaRole::CANDIDATE;
    // election_ticks_ = 0;
    election_tick_trigger_.reset(ELECTION_TIMEOUT);

    // 如果只有一个节点的话，就直接当选
    if (members_.size() == 1) {
        become_leader();
        return;
    }

    // 给所有 peer 发 RequestVote
    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;
        send_request_vote_to(member);
    }
}

void RaftNode::send_request_vote_to(const PeerMember& member) {
    RaftMessage msg;
    msg.type = RaftMessageType::REQUEST_VOTE;
    msg.target = member;
    msg.vote_param = {
        .from_replica_id = self_id_,
        .to_replica_id = member.replica_id,
        .term = current_term_,
        .last_log_term = last_log_term(),
        .last_log_index = last_log_index(),
    };
    pending_messages_.push_back(std::move(msg));
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
std::pair<Status, LogIndex> RaftNode::propose(WriteOpType op, const Key& key,
                                              const Value& value) {
    if (role_ != ReplicaRole::LEADER) {
        return {Status{StatusCode::NOT_LEADER, "not leader"}, -1};
    }
    LogIndex new_commit_idx = last_log_index() + 1;

    LogEntry entry{
        .term = current_term_,
        .index = last_log_index() + 1,
        .op_type = op,
        .key = key,
        .value = value,
    };
    log_entries_.push_back(std::move(entry));

    // 广播给所有 follower
    broadcast_append_entries();

    if (role_ == ReplicaRole::LEADER and members_.size() == 1) {
        // 如果自己是leader，并且整个group只有自己一个节点的话，那就更新下commit_idx
        try_update_commit_index();
    }

    return {Status::OK(), new_commit_idx};
}

/*
注意，在广播的时候，我们会把每一个follower的 从他们next_idx开始到最新的消息
都发送给他们。 具体是会放到pending_message队列里面。
所以每一次广播之后，我们的pending_message就会更新。
*/
void RaftNode::broadcast_append_entries() {
    if (role_ != ReplicaRole::LEADER) return;

    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;

        if (!next_index_.count(member.replica_id)) {
            next_index_[member.replica_id] = last_log_index() + 1;
        }
        if (!match_index_.count(member.replica_id)) {
            match_index_[member.replica_id] = 0;
        }

        LogIndex next_idx = next_index_[member.replica_id];
        send_append_entries_to(member, next_idx);
    }
}

void RaftNode::send_append_entries_to(const PeerMember& member,
                                      LogIndex next_index) {
    LogIndex prev_log_index = next_index - 1;
    Term prev_log_term =
        (prev_log_index == 0) ? 0 : log_entries_[prev_log_index - 1].term;

    AppendEntriesParam param{
        .from_replica_id = self_id_,
        .to_replica_id = member.replica_id,
        .term = current_term_,
        .prev_log_index = prev_log_index,
        .prev_log_term = prev_log_term,
        .leader_commit = commit_index_,
    };

    for (LogIndex idx = next_index; idx <= last_log_index(); ++idx) {
        param.entries.push_back(log_entries_[idx - 1]);
    }

    RaftMessage msg;
    msg.type = RaftMessageType::APPEND_ENTRIES;
    msg.target = member;
    msg.append_param = std::move(param);
    pending_messages_.push_back(std::move(msg));
}

void RaftNode::handle_request_vote(const RequestVoteParam& param,
                                   RequestVoteResult& result) {
    result.term = current_term_;
    result.vote_granted = false;

    if (param.term < current_term_) {
        return;
    }

    if (param.term > current_term_) {
        // 这里这个函数会把自己的vote_for_给清空掉，会不会导致投了很多次票
        // 但是应该没有问题，毕竟是这种情况应该是发生在多次投票里面，他们的term不一样
        // 既然不一样的话，肯定是最终term最大的那一个去当的leader了，别的会自动变成follower
        become_follower(param.term);
        result.term = current_term_;
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
        result.vote_granted = true;
        voted_for_ = param.from_replica_id;
        // election_ticks_ = 0;
        election_tick_trigger_.reset(ELECTION_TIMEOUT);
    }
}

// 这个是leadr发过来，raftnode作为follower/cacdidate做的handle
void RaftNode::handle_append_entries(const AppendEntriesParam& param,
                                     AppendEntriesResult& result) {
    result.success = false;
    result.term = current_term_;

    if (param.term < current_term_) {
        return;
    }

    if (param.term > current_term_ || role_ != ReplicaRole::FOLLOWER) {
        // 这里的判断条件是，只要role不是FOLLOWER，就会become，但是如果term和param的term是相等的
        // 然后prev_log_index不一样呢？ 这个时候不需要比较一下index吗？
        // 这里并不需要，毕竟append 不需要去干关于选举方面的事情，
        become_follower(param.term);
    }

    result.term = current_term_;

    if (param.prev_log_index > 0) {
        if (last_log_index() < param.prev_log_index) {
            return;
        }
        const LogEntry& prev_entry = log_entries_[param.prev_log_index - 1];
        if (prev_entry.term != param.prev_log_term) {
            return;
        }
    }

    // 把自己的时间reset一下， 选举的
    // election_ticks_ = 0;
    election_tick_trigger_.reset(ELECTION_TIMEOUT);

    if (param.entries.empty()) {
        // 这里是心跳
    } else {
        // 日志复制
        for (const LogEntry& entry : param.entries) {
            LogIndex index = entry.index;
            if (index <= last_log_index()) {
                if (log_entries_[index - 1].term != entry.term) {
                    log_entries_.resize(index - 1);
                    log_entries_.push_back(entry);
                }
            } else {
                log_entries_.push_back(entry);
            }
        }
    }

    // 不管是否有entry，也就是不管是日志追加还是心跳，都会需要更新commit_idx
    if (param.leader_commit > commit_index_) {
        commit_index_ = std::min(param.leader_commit, last_log_index());
    }

    result.success = true;
    result.term = current_term_;
}

void RaftNode::handle_vote_response(const ReplicaID& from,
                                    const RequestVoteResult& result) {
    // 已经不是 CANDIDATE 了，就直接忽略之前的发起内容
    if (role_ != ReplicaRole::CANDIDATE) return;

    if (result.term > current_term_) {
        become_follower(result.term);
        return;
    }

    if (!result.vote_granted) return;

    granted_vote_count_++;
    int limit = static_cast<int>(members_.size()) / 2 + 1;
    if (granted_vote_count_ >= limit) {
        become_leader();
    }
}

// 这里的from是代表的从form那边返回的response。
// 写到一半的时候脑子混了
// 这个是raftnode作为leader，收到了来自别的replica的回应， 自己的handle
void RaftNode::handle_append_response(const ReplicaID& from,
                                      const AppendEntriesResult& result) {
    if (role_ != ReplicaRole::LEADER) return;

    if (result.term > current_term_) {
        become_follower(result.term);
        return;
    }

    if (result.success) {
        // 复制成功，更新 match_index 和 next_index
        match_index_[from] =
            last_log_index();  // 这里目前是同步，所以应该没有问题，但是以后如果要改成了异步，需要留意一下，这里应该就有问题了。
        next_index_[from] = last_log_index() + 1;
        try_update_commit_index();
    } else {
        // prev_log 对不上，往前回退
        auto it = next_index_.find(from);
        if (it != next_index_.end() && it->second > 1) {
            --it->second;
        }
    }
}

///////////////////////// extract
std::vector<RaftMessage> RaftNode::extract_messages() {
    std::vector<RaftMessage> messages;
    messages.swap(pending_messages_);
    return messages;
}

std::vector<LogEntry> RaftNode::extract_committed_entries() {
    std::vector<LogEntry> entries;
    for (LogIndex i = last_applied_ + 1; i <= commit_index_; i++) {
        entries.push_back(log_entries_[i - 1]);
    }
    return entries;
}

void RaftNode::advance_last_applied(LogIndex applied) {
    if (applied > last_applied_) {
        last_applied_ = applied;
    }
}

void RaftNode::become_follower(Term later_term) {
    assert(later_term >= current_term_);

    if (later_term > current_term_) {
        voted_for_.reset();
    }
    current_term_ = later_term;
    role_ = ReplicaRole::FOLLOWER;
    // election_ticks_ = 0;
    election_tick_trigger_.reset(ELECTION_TIMEOUT);
}

void RaftNode::become_leader() {
    role_ = ReplicaRole::LEADER;
    // heartbeat_ticks_ = election_ticks_ = 0;
    election_tick_trigger_.reset(ELECTION_TIMEOUT);
    heartbeat_tick_trigger_.clear();

    // TODO 为什么当上了leader之后需要把这些全都初始化呢？ 保留原来的值不行吗？
    for (const PeerMember& member : members_) {
        if (member.replica_id == self_id_) continue;
        next_index_[member.replica_id] = last_log_index() + 1;
        match_index_[member.replica_id] = 0;
    }

    // 追加 no-op entry
    LogEntry none_entry{
        .term = current_term_,
        .index = last_log_index() + 1,
        .op_type = WriteOpType::NONE,
        .key = {},
        .value = {},
    };
    log_entries_.push_back(std::move(none_entry));

    // 立即广播（含 no-op），相当于心跳 + 日志复制合一
    broadcast_append_entries();

    if (members_.size() == 1) {
        try_update_commit_index();
    }
}

// raftnode 作为leader，需要更新自己的commit_idx
// 当收到了follower们关于日志复制的回应时，就调用一下这个函数，去更新自己的commit_idx
void RaftNode::try_update_commit_index() {
    for (LogIndex idx = commit_index_ + 1; idx <= last_log_index(); ++idx) {
        // TODO 这里将来需要check一下这个term是否需要和当前的term一样吗？
        // 但是好像leader是可以提交上一个leader没有提交的内容吧，所以好像不用

        // 原来如此: leader 确实可以提交前一个 leader 未提交的
        // entry，但不是"直接"提交。Raft 的规则是：只有当当前 term 的 entry
        // 被多数派确认后，commit_index 才会推进，此时之前 term 的 entry 会因为
        // commit_index 的递增而被顺带提交（ trace_commit_log_entries 从
        // last_applied_ 推进到 commit_index_ ，包含了之前 term 的 entry）。
        if (log_entries_[idx - 1].term != current_term_) {
            continue;
        }

        int success_cnt = 1;
        for (const auto& member : members_) {
            if (member.replica_id == self_id_) {
                continue;
            }
            if (auto it = match_index_.find(member.replica_id);
                it != match_index_.end()) {
                if (it->second >= idx) success_cnt++;
            } else {
                WARN("...");
            }
        }

        int limit_cnt = static_cast<int>(members_.size()) / 2 + 1;
        if (success_cnt >= limit_cnt) {
            commit_index_ = idx;
        }
    }
}

#undef ELECTION_TIMEOUT

}  // namespace adviskv::storage
