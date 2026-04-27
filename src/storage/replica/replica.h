#pragma once

#include <google/protobuf/stubs/port.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include "common.pb.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "storage.pb.h"
#include "storage/engine/kv_engine.h"
#include "storage/model/param.h"
#include "storage/raft/raft_callback.h"
#include "storage/utility/timer.h"
namespace adviskv::storage {

class Replica {
   public:
    TableID get_table_id() const { return shard_id_.table_id; }
    ShardID get_shard_id() const { return shard_id_; }

    Status put(const PutParam& param);
    Status get(const GetParam& param, Value& value);

    Status handle_request_vote(const RequestVoteParam& param,
                               RequestVoteResult& result);
    Status handle_append_entries(const AppendEntriesParam& param,
                                 AppendEntriesResult& result);

   private:
    Term get_last_log_term() const;
    LogIndex get_last_log_index() const;
    bool later_than_other_replica(Term other_last_log_term,
                                  LogIndex other_last_log_index) const;
    Status become_follower(Term later_term);

        Status become_leader();

    void execute_election();  // 处理定时选举的入口
    Status send_member_request_vote(const PeerMember& member,
                                    int32_t generation);
    Status get_member_vote_response(const PeerMember& member, int32_t generation,
                                const RequestVoteResult& result);

    void execute_heartbeat();  // 处理心跳的入口

    Status send_members_append_entries();
    Status try_update_commit_index();
    Status apply_log_entry(const LogEntry& entry);
    Status trace_commit_log_entries();

   private:
    friend class ReplicaManager;

    Status init(const ReplicaInitParam& param);

    ShardID shard_id_;
    ReplicaID replica_id_;
    ReplicaRole role_;

    std::unique_ptr<KVEngine> engine_;

    ////////// raft 相关内容

    Term current_term_;  // 当前term

    int32_t election_generation_;  // 选举时期
    int32_t granted_vote_count_;   // 获得的票数

    std::vector<LogEntry> log_entries_;  // log entires
    std::vector<PeerMember> members_;    // 成员， 包括了自己

    std::optional<ReplicaID> voted_for_;  // 投给了谁

    RaftSender raft_sender_;  // 负责选举和entry的发送

    // 两个timer，负责定时运行的。
    TimerPtr election_timer_;
    TimerPtr heartbeat_timer_;

    LogIndex commit_index_{0};
    LogIndex last_applied_{0};

    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> next_index_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> match_index_;
};

}  // namespace adviskv::storage
