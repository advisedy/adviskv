#include <cassert>
#include <utility>

#include "common/log.h"
#include "storage/raft/core/raft_core.h"

namespace adviskv::storage {

void RaftCore::tick(RaftEffects& effects) {
    if (ensure_ready().fail()) return;

    if (election_.is_leader()) {
        heartbeat_tick_trigger_.tick([&]() { broadcast_append_entries(effects); });
        maybe_promote_ready_learner(effects);
    } else {
        election_tick_trigger_.tick([&]() { become_pre_candidate(effects); });
    }
}

void RaftCore::become_pre_candidate(RaftEffects& effects) {
    if (ensure_ready().fail()) return;
    if (!membership_.is_voter(self_id_)) return;
    if (election_.is_leader()) return;

    clear_recent_leader_active();
    ReplicaRole old_role = election_.role();
    election_.become_pre_candidate();
    LOG_INFO(
            "[RaftCore Election] replica:{} become pre-candidate, old_role:{}, current_term:{}, "
            "pre_vote_term:{}, last_log_index:{}, last_log_term:{}, pre_vote_granted:{}",
            self_id_.to_string(), to<int32>(old_role), election_.current_term(), election_.pre_vote_term(),
            raft_log_.last_log_index(), raft_log_.last_log_term(), election_.pre_vote_granted_count());

    heartbeat_tick_trigger_.stop();
    election_tick_trigger_.reset();

    if (membership_.has_quorum(election_.pre_vote_granted_count())) {
        become_candidate(effects);
        return;
    }

    for (const PeerMember& member : membership_.voters()) {
        if (member.replica_id == self_id_) continue;
        send_pre_vote_to(member, effects);
    }
}

void RaftCore::send_pre_vote_to(const PeerMember& member, RaftEffects& effects) {
    RaftMessage msg;
    msg.type = RaftMessageType::PRE_VOTE;
    msg.target = member;
    msg.pre_vote_param.from_replica_id = self_id_;
    msg.pre_vote_param.to_replica_id = member.replica_id;
    msg.pre_vote_param.term = election_.pre_vote_term();
    msg.pre_vote_param.last_log_index = raft_log_.last_log_index();
    msg.pre_vote_param.last_log_term = raft_log_.last_log_term();
    effects.messages.push_back(std::move(msg));
}

void RaftCore::become_candidate(RaftEffects& effects) {
    if (ensure_ready().fail()) return;
    if (!membership_.is_voter(self_id_)) return;
    clear_recent_leader_active();
    Term previous_term = election_.current_term();
    election_.become_candidate();
    LOG_INFO(
            "[RaftCore Election] replica:{} start become candidate, previous_term:{}, current_term:{}, "
            "last_log_index:{}, last_log_term:{}, granted_vote_count:{}",
            self_id_.to_string(), previous_term, election_.current_term(), raft_log_.last_log_index(),
            raft_log_.last_log_term(), election_.granted_vote_count());
    record_hard_state(effects);

    heartbeat_tick_trigger_.stop();
    election_tick_trigger_.reset();

    // 如果只有一个节点的话，就直接当选
    if (membership_.has_quorum(election_.granted_vote_count())) {
        become_leader(effects);
        return;
    }

    // 给所有 peer 发 RequestVote
    for (const PeerMember& member : membership_.voters()) {
        if (member.replica_id == self_id_) continue;
        send_request_vote_to(member, effects);
    }
}

void RaftCore::send_request_vote_to(const PeerMember& member, RaftEffects& effects) {
    RaftMessage msg;
    msg.type = RaftMessageType::REQUEST_VOTE;
    msg.target = member;
    msg.vote_param.from_replica_id = self_id_;
    msg.vote_param.to_replica_id = member.replica_id;
    msg.vote_param.term = election_.current_term();
    msg.vote_param.last_log_index = raft_log_.last_log_index();
    msg.vote_param.last_log_term = raft_log_.last_log_term();
    effects.messages.push_back(std::move(msg));
}

void RaftCore::handle_pre_vote(const PreVoteParam& param, PreVoteResult& result, RaftEffects& effects) {
    UNUSED(effects);
    result.term = election_.current_term();
    result.vote_granted = false;

    if (!membership_.is_voter(self_id_)) {
        return;
    }

    if (!membership_.is_voter(param.from_replica_id)) {
        return;
    }

    // param.term 是对方“如果正式选会用的 term”，一般是 current_term+1。
    // 比这个还旧就没必要给了。
    if (param.term < election_.current_term() + 1) {
        LOG_DEBUG(
                "[RaftCore Election] replica:{} reject PreVote with stale prospective term, from:{}, "
                "request_term:{}, current_term:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term());
        return;
    }

    Status ready_status = ensure_ready();
    if (ready_status.fail()) {
        LOG_WARN(
                "[RaftCore Election] replica:{} reject PreVote because raft is not ready, from:{}, "
                "request_term:{}, current_term:{}, status:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term(),
                ready_status.to_string());
        return;
    }

    if (election_.is_leader()) {
        LOG_DEBUG("[RaftCore Election] replica:{} reject PreVote because self is the current leader, from:{}, "
                  "request_term:{}, current_term:{}",
                  self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term());
        return;
    }

    if (election_.is_follower() && is_recent_leader_active()) {
        LOG_DEBUG("[RaftCore Election] replica:{} reject PreVote because recent leader is still active, "
                  "from:{}, request_term:{}, current_term:{}",
                  self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term());
        return;
    }

    // 如果比人家的新 （是一定会新，不包括相等）
    if (later_than_other(param.last_log_term, param.last_log_index)) {
        LOG_DEBUG(
                "[RaftCore Election] replica:{} reject PreVote because log is more up-to-date, from:{}, "
                "request_last_log:{}:{}, self_last_log:{}:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.last_log_index, param.last_log_term,
                raft_log_.last_log_index(), raft_log_.last_log_term());
        return;
    }

    result.vote_granted = true;
    LOG_DEBUG("[RaftCore Election] replica:{} grant PreVote to {}, current_term:{}, request_term:{}",
              self_id_.to_string(), param.from_replica_id.to_string(), election_.current_term(), param.term);
}

void RaftCore::handle_pre_vote_response(const ReplicaID& from, const PreVoteResult& result, RaftEffects& effects) {
    Status ready_status = ensure_ready();
    if (ready_status.fail()) {
        return;
    }

    // 已经不是 PRE_CANDIDATE 了，就直接忽略之前的发起内容
    if (!election_.is_pre_candidate()) {
        LOG_DEBUG(
                "[RaftCore Election] replica:{} ignore PreVote response because role is not pre-candidate, "
                "from:{}, role:{}",
                self_id_.to_string(), from.to_string(), to<int32>(election_.role()));
        return;
    }

    LOG_DEBUG(
            "[RaftCore Election] replica:{} receive PreVote response from:{}, current_term:{}, "
            "pre_vote_term:{}, response_term:{}, vote_granted:{}",
            self_id_.to_string(), from.to_string(), election_.current_term(), election_.pre_vote_term(), result.term,
            result.vote_granted);

    if (result.term > election_.current_term()) {
        become_follower(result.term, effects);
        return;
    }

    if (!result.vote_granted) {
        return;
    }

    if (!election_.record_pre_vote_granted_from(from)) {
        return;
    }

    LOG_DEBUG("[RaftCore Election] replica:{} pre-vote count++ to {} after grant from {}", self_id_.to_string(),
              election_.pre_vote_granted_count(), from.to_string());

    if (membership_.has_quorum(election_.pre_vote_granted_count())) {
        become_candidate(effects);
    }
}

void RaftCore::handle_request_vote(const RequestVoteParam& param, RequestVoteResult& result, RaftEffects& effects) {
    result.term = election_.current_term();
    result.vote_granted = false;

    // 这边对于recovering状态，我们是会去更新它的current
    // term的，以防止在恢复正常之后可能会接收到一些比较旧的term的节点的消息，并且做出回复
    // if (ensure_not_faulted_unlocked().fail()) {
    //     return;
    // }

    if (!membership_.is_voter(self_id_)) {
        LOG_WARN(
                "[RaftCore Election] replica:{} reject RequestVote because self is "
                "not voter, from:{}, term:{}, current_term:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term());
        return;
    }
    if (!membership_.is_voter(param.from_replica_id)) {
        LOG_WARN(
                "[RaftCore Election] replica:{} reject RequestVote from non-voter "
                "replica:{}, term:{}, current_term:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term());
        return;
    }

    if (param.term < election_.current_term()) {
        return;
    }

    if (param.term > election_.current_term()) {
        // 这里这个函数会把自己的vote_for_给清空掉，会不会导致投了很多次票
        // 但是应该没有问题，毕竟是这种情况应该是发生在多次投票里面，他们的term不一样
        // 既然不一样的话，肯定是最终term最大的那一个去当的leader了，别的会自动变成follower
        LOG_INFO(
                "[RaftCore Election] replica:{} receive higher-term RequestVote, from:{}, "
                "request_term:{}, current_term:{}, candidate_last_log_index:{}, candidate_last_log_term:{}, "
                "self_last_log_index:{}, self_last_log_term:{}, current_role:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term(),
                param.last_log_index, param.last_log_term, raft_log_.last_log_index(), raft_log_.last_log_term(),
                to<int32>(election_.role()));
        become_follower(param.term, effects);
        result.term = election_.current_term();
    }

    Status ready_status = ensure_ready();
    if (ready_status.fail()) {
        LOG_WARN(
                "[RaftCore Election] replica:{} reject RequestVote because raft is not ready, "
                "from:{}, request_term:{}, current_term:{}, status:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term(),
                ready_status.to_string());
        return;
    }

    // 如果比人家的新 （是一定会新，不包括相等）
    if (later_than_other(param.last_log_term, param.last_log_index)) {
        return;
    }

    // 没有投过票，或者投过了，还是这个人
    // 如果投过了这个同一个人的话，其实这里vote_granted是true还是false都无所谓吧，反正对方已经拿到票了。
    // 保障一致性，还是设置成true把

    if (election_.grant_vote_to(param.from_replica_id)) {
        LOG_DEBUG("replica:{} vote to {}, current_term:{}", self_id_.to_string(), param.from_replica_id.to_string(),
                  election_.current_term());
        record_hard_state(effects);
        result.vote_granted = true;
        election_tick_trigger_.clear();
    } else {
        auto&& voted_for = election_.voted_for();
        LOG_DEBUG(
                "[RaftCore Election] replica:{} reject RequestVote because already voted for another replica, "
                "from:{}, term:{}, current_term:{}, voted_for:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term(),
                voted_for.has_value() ? voted_for->to_string() : "none");
    }
}

void RaftCore::handle_vote_response(const ReplicaID& from, const RequestVoteResult& result, RaftEffects& effects) {
    Status ready_status = ensure_ready();
    if (ready_status.fail()) {
        LOG_DEBUG(
                "[RaftCore Election] replica:{} ignore RequestVote response because raft is not ready, "
                "from:{}, status:{}",
                self_id_.to_string(), from.to_string(), ready_status.to_string());
        return;
    }

    LOG_DEBUG(
            "[RaftCore Election] replica:{} receive RequestVote response from:{}, role:{}, "
            "current_term:{}, response_term:{}, vote_granted:{}",
            self_id_.to_string(), from.to_string(), to<int32>(election_.role()), election_.current_term(), result.term,
            result.vote_granted);

    // 已经不是 CANDIDATE 了，就直接忽略之前的发起内容
    if (!election_.is_candidate()) {
        LOG_DEBUG(
                "[RaftCore Election] replica:{} ignore RequestVote response because current role is not candidate, "
                "from:{}, role:{}",
                self_id_.to_string(), from.to_string(), to<int32>(election_.role()));
        return;
    }

    if (result.term > election_.current_term()) {
        become_follower(result.term, effects);
        return;
    }

    if (result.term < election_.current_term()) {
        return;
    }

    if (!result.vote_granted) {
        LOG_DEBUG("[RaftCore Election] candidate replica:{} vote rejected by replica:{}, term:{}", self_id_.to_string(),
                  from.to_string(), result.term);
        return;
    }

    if (!election_.record_vote_granted_from(from)) {
        return;
    }
    LOG_DEBUG(
            "candidate replica:{} get vote response from replica:{}, self vote "
            "count++ to {}",
            self_id_.to_string(), from.to_string(), election_.granted_vote_count());

    if (membership_.has_quorum(election_.granted_vote_count())) {
        become_leader(effects);
    }
}

void RaftCore::become_follower(Term later_term, RaftEffects& effects) {
    assert(later_term >= election_.current_term());

    LOG_INFO("[RaftCore Election] replica:{} become follower, old_role:{}, old_term:{}, new_term:{}",
             self_id_.to_string(), to<int32>(election_.role()), election_.current_term(), later_term);

    clear_recent_leader_active();
    heartbeat_tick_trigger_.stop();
    election_tick_trigger_.reset();

    if (election_.become_follower(later_term)) {
        record_hard_state(effects);
    }
}

void RaftCore::become_leader(RaftEffects& effects) {
    if (ensure_ready().fail()) return;
    if (!membership_.is_voter(self_id_)) return;

    LOG_INFO("[RaftCore Election] replica:{} become leader, term:{}, last_log_index:{}, last_log_term:{}",
             self_id_.to_string(), election_.current_term(), raft_log_.last_log_index(), raft_log_.last_log_term());
    clear_recent_leader_active();
    election_.become_leader();

    election_tick_trigger_.stop();
    heartbeat_tick_trigger_.reset();

    // TODO 为什么当上了leader之后需要把这些全都初始化呢？ 保留原来的值不行吗？
    replication_.reset_for_leader();

    append_new_entry(ProposeParam::noop(), effects);
    // 立即广播（含 no-op），相当于心跳 + 日志复制合一
    broadcast_append_entries(effects);

    if (membership_.has_quorum(1)) {
        try_update_commit_index(effects);
    }
}

}  // namespace adviskv::storage