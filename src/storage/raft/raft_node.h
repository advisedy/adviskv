#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/func.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
namespace adviskv::storage {

// using TickFunc = void (*)();
using TickFunc = std::function<void()>;

// 这个就是一个计数触发器
// 手动在外面保证传进来的limit_cnt是非负吧。
class TickTrigger {
   public:
    TickTrigger(int32_t limit_cnt, TickFunc func)
        : limit_cnt_(limit_cnt), func_(func) {}

    void tick() {
        cur_cnt_++;
        if (cur_cnt_ >= limit_cnt_) {
            cur_cnt_ = 0;
            func_();
        }
    }

    void reset(int32_t limit_cnt) {
        cur_cnt_ = 0;
        limit_cnt_ = limit_cnt;
        if (cur_cnt_ >= limit_cnt_) {
            cur_cnt_ = 0;
            func_();
        }
    }

    void clear() { cur_cnt_ = 0; }

   private:
    int32_t cur_cnt_{0};
    int32_t limit_cnt_;
    TickFunc func_;
};

class RaftNode {
   public:
    RaftNode(const ReplicaID& self_id, const std::vector<PeerMember>& members);

    void tick();

    // 处理外层的写请求
    // 这里返回值第一个是Status，
    // 第二个是commit之后，新的commit_index应该对应是多少
    std::pair<Status, LogIndex> propose(WriteOpType op, const Key& key,
                                        const Value& value);

    // 处理来自storage_service_impl的RPC的请求
    void handle_request_vote(const RequestVoteParam& param,
                             RequestVoteResult& result);
    void handle_append_entries(const AppendEntriesParam& param,
                               AppendEntriesResult& result);

    // 当replica通过flush_message去发送消息之后， 会返回过来结果
    // raft_node会再去处理这个，response
    void handle_vote_response(const ReplicaID& from,
                              const RequestVoteResult& result);
    void handle_append_response(const ReplicaID& from,
                                const AppendEntriesResult& result);

    std::vector<RaftMessage>
    extract_messages();  // 提取raft这边产生的message，replica读取了之后会进行这些操作。
                         // 提取pending_message， 主要是来自become_candidate 和
                         // 广播函数。
    std::vector<LogEntry>
    extract_committed_entries();  // 提取那些已提交但是还未 apply 的日志

    ReplicaRole role() const { return role_; }
    Term current_term() const { return current_term_; }
    LogIndex commit_index() const { return commit_index_; }
    LogIndex last_applied() const { return last_applied_; }
    LogIndex last_log_index() const;
    Term last_log_term() const;
    bool is_leader() const { return role_ == ReplicaRole::LEADER; }

    // 外部更新 last_applied（apply 完成后调用）
    void advance_last_applied(LogIndex applied);

   private:
    void become_follower(Term later_term);
    void become_leader();
    void become_candidate();

    void try_update_commit_index();
    bool later_than_other(Term other_term, LogIndex other_index) const;

    // 对于raftNOde来说，发送RPC的申请交给了外包的replica，自己只需要负责放到vector里面就好了
    // replica那边会自动拿取队列里面的内容的
    void send_request_vote_to(const PeerMember& member);
    void send_append_entries_to(const PeerMember& member, LogIndex next_index);
    void broadcast_append_entries();

    ReplicaID self_id_;
    ReplicaRole role_{ReplicaRole::FOLLOWER};
    Term current_term_{0};
    std::optional<ReplicaID> voted_for_;
    LogIndex commit_index_{0};
    LogIndex last_applied_{0};

    // 选举
    int32_t election_generation_{0};
    int32_t granted_vote_count_{0};

    std::vector<LogEntry> log_entries_;
    std::vector<PeerMember> members_;

    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> next_index_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> match_index_;

    // tick 计数器（替代 Timer）
    // int32_t election_ticks_{0};
    // int32_t heartbeat_ticks_{0};
    TickTrigger election_tick_trigger_;
    TickTrigger heartbeat_tick_trigger_;

    // 待发消息队列
    std::vector<RaftMessage> pending_messages_;
};

}  // namespace adviskv::storage
