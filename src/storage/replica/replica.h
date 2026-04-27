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


    void execute_election();// 处理定时选举的入口
    Status send_member_request_vote(const PeerMember& member,
                                    int32_t generation);
    Status handle_vote_response(const PeerMember& member, int32_t generation,
                                const RequestVoteResult& result);

    void on_heartbeat_timeout();
    void execute_heartbeat();

   private:
    friend class ReplicaManager;

    Status init(const ReplicaInitParam& param);

    ShardID shard_id_;
    ReplicaID replica_id_;
    ReplicaRole role_;
    Term current_term_;

    int32_t election_generation_;
    int32_t granted_vote_count_;

    std::optional<ReplicaID> voted_for_;

    std::vector<LogEntry> log_entries_;
    std::vector<PeerMember> members_;
    std::unique_ptr<KVEngine> engine_;

    RaftSender raft_sender_;

    TimerPtr election_timer_;
    TimerPtr heartbeat_timer_;
};

}  // namespace adviskv::storage
