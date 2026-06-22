#include "storage/raft/core/raft_core.h"

namespace adviskv::storage {

void RaftCore::update_raft_meta(const RaftMeta& meta) {
    election_.update_meta(meta);
}

void RaftCore::update_log_entries(const std::vector<LogEntry>& entries) {
    raft_log_.update_entries(entries);
}

void RaftCore::enter_recovering() {
    state_ = State::RECOVERING;
    election_.become_follower(election_.current_term());
    election_tick_trigger_.stop();
    heartbeat_tick_trigger_.stop();
}

void RaftCore::finish_recovering() {
    if (state_ != State::RECOVERING) return;

    state_ = State::READY;
    election_tick_trigger_.reset();
}

}  // namespace adviskv::storage
