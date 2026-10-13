#include "storage/raft/core/raft_core.h"

namespace adviskv::storage {

bool RaftCore::has_committed_current_term_entry() const {
    return raft_apply_.has_committed_current_term_entry(election_.current_term());
}

Status RaftCore::build_read_quorum_check(RaftEffects& effects, LogIndex& read_index, Term& read_term,
                                       std::vector<ReplicaID>& voter_ids, uint64 read_quorum_seq) {
    RETURN_IF_INVALID_STATUS(ensure_readable_leader())

    read_term = election_.current_term();
    read_index = raft_apply_.commit_index();

    voter_ids.clear();
    for (const RaftMember& member : membership_.raft_members()) {
        if (is_raft_voter(member.member_type)) {
            voter_ids.push_back(member.peer.replica_id);
        }
    }

    const LogIndex prev_log_index = raft_log_.last_log_index();
    const Term prev_log_term = raft_log_.last_log_term();
    for (const RaftMember& member : membership_.raft_members()) {
        if (!is_raft_voter(member.member_type)) continue;
        if (member.peer.replica_id == self_id_) continue;

        AppendEntriesParam param;
        param.from_replica_id = self_id_;
        param.to_replica_id = member.peer.replica_id;
        param.term = read_term;
        param.prev_log_index = prev_log_index;
        param.prev_log_term = prev_log_term;
        param.leader_commit = raft_apply_.commit_index();
        param.read_quorum_seq = read_quorum_seq;

        RaftMessage msg;
        msg.type = RaftMessageType::APPEND_ENTRIES;
        msg.target = member.peer;
        msg.append_param = std::move(param);
        effects.messages.push_back(std::move(msg));
    }
    return Status::OK();
}

Status RaftCore::confirm_read_index(LogIndex read_index, Term read_term) const {
    RETURN_IF_INVALID_STATUS(ensure_readable_leader())

    if (read_term != election_.current_term()) {
        return Status::NOT_LEADER("leader term changed during read");
    }
    if (raft_apply_.commit_index() < read_index) {
        return Status::READ_INDEX_NOT_READY("commit index moved behind read index");
    }
    return Status::OK();
}

}  // namespace adviskv::storage
