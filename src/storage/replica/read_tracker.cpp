#include "storage/replica/read_tracker.h"

#include <algorithm>
#include <utility>

#include "common/defer.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/model/model.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

bool read_quorum_response_counts(ReadRequestState& read, const ReplicaID& from, uint64 response_seq,
                                 Term response_term) {
    if (response_seq != read.read_seq) return false;
    if (response_term != read.read_quorum_term) return false;
    const std::vector<ReplicaID>& voters = read.voter_ids;
    if (std::find(voters.begin(), voters.end(), from) == voters.end()) return false;
    return read.confirmed_voters.insert(from).second;
}

ReadTracker::ReadTracker(RaftCore& raft_core, StateMachine& state_machine,
                         ReplicaMessageDispatcher& message_dispatcher, CompletionManager& completions,
                         FaultReporter fault_reporter)
        : raft_core_(raft_core),
          state_machine_(state_machine),
          message_dispatcher_(message_dispatcher),
          completions_(completions),
          fault_reporter_(std::move(fault_reporter)) {}

std::shared_ptr<ReadRequestState> ReadTracker::new_request(const Key& key, CompletionManager::Ticket ticket) {
    auto state = std::make_shared<ReadRequestState>();
    state->key = key;
    state->ticket = ticket;
    state->deadline = std::chrono::steady_clock::now() + kReadTimeout;
    return state;
}

Status ReadTracker::establish(const std::shared_ptr<ReadRequestState>& state) {
    state->membership_epoch = membership_epoch_;
    state->read_seq = next_read_seq_++;

    RaftEffects effects;
    Status status = raft_core_.build_read_quorum_check(effects, state->read_index, state->read_term,
                                                     state->voter_ids, state->read_seq);
    if (status.fail()) {
        return status;
    }

    state->read_quorum_term = state->read_term;
    pending_reads_.emplace(state->read_seq, state);

    Status send_status = message_dispatcher_.async_send(std::move(effects.messages));
    if (send_status.fail()) {
        LOG_WARN("[ReadTracker] read quorum async_send failed, status:{}", send_status.to_string());
    }
    return Status::OK();
}

void ReadTracker::on_read_quorum_response(const ReplicaID& from, uint64 read_seq, Term response_term) {
    auto it = pending_reads_.find(read_seq);
    if (it == pending_reads_.end() || it->second->done) {
        return;
    }
    auto& state = it->second;
    if (read_quorum_response_counts(*state, from, read_seq, response_term)) {
        ++state->confirmed_count;
        ADVISKV_METRICS_COUNTER("storage_replica_read_quorum_vote");
    }
}

void ReadTracker::advance() {
    if (pending_reads_.empty()) return;
    if (checking_) return;
    checking_ = true;
    Defer([&] { checking_ = false; });

    const Term current_term = raft_core_.current_term();
    const bool is_leader = raft_core_.is_leader();
    const uint64_t current_epoch = membership_epoch_;
    const auto now = std::chrono::steady_clock::now();

    for (auto it = pending_reads_.begin(); it != pending_reads_.end();) {
        auto& state = it->second;
        if (state->done) {
            it = pending_reads_.erase(it);
            continue;
        }

        if (!is_leader || current_term != state->read_term || current_epoch != state->membership_epoch) {
            state->done = true;
            state->status = Status::NOT_LEADER("leader or membership changed during read");
            IGNORE_RESULT(completions_.finish(state->ticket, state->status))
            it = pending_reads_.erase(it);
            continue;
        }

        if (now >= state->deadline) {
            state->done = true;
            state->status = Status::NOT_LEADER("read index quorum not reached before timeout");
            IGNORE_RESULT(completions_.finish(state->ticket, state->status))
            it = pending_reads_.erase(it);
            continue;
        }

        const int limit = to<int>(state->voter_ids.size()) / 2 + 1;
        if (state->confirmed_count < limit) {
            ++it;
            continue;
        }
        if (raft_core_.last_applied() < state->read_index) {
            ++it;
            continue;
        }
        Status confirm_status = raft_core_.confirm_read_index(state->read_index, state->read_term);
        if (confirm_status.fail()) {
            state->done = true;
            state->status = confirm_status;
            IGNORE_RESULT(completions_.finish(state->ticket, state->status))
            it = pending_reads_.erase(it);
            continue;
        }

        Status get_status = state_machine_.get(state->key, state->value);
        if (get_status.fail() && get_status.code() != StatusCode::KEY_NOT_FOUND) {
            fault_reporter_.report(Status::ERROR("replica fault"));
            state->done = true;
            state->status = get_status;
            IGNORE_RESULT(completions_.finish(state->ticket, state->status))
            it = pending_reads_.erase(it);
            return;
        }
        state->done = true;
        state->status = get_status;
        IGNORE_RESULT(completions_.finish(state->ticket, state->status))
        ADVISKV_METRICS_COUNTER("storage_replica_read_completed");
        it = pending_reads_.erase(it);
    }
}

void ReadTracker::fail_all(const Status& status) {
    for (auto& [seq, state] : pending_reads_) {
        if (!state->done) {
            state->done = true;
            state->status = status;
            IGNORE_RESULT(completions_.finish(state->ticket, status))
        }
    }
    pending_reads_.clear();
}

}
