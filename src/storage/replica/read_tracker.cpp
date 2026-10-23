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

ReadTracker::ReadTracker(RaftCore& raft_core, StateMachine& state_machine, ReplicaMessageDispatcher& message_dispatcher,
                         FaultReporter fault_reporter)
        : raft_core_(raft_core),
          state_machine_(state_machine),
          message_dispatcher_(message_dispatcher),
          fault_reporter_(std::move(fault_reporter)) {}

Status ReadTracker::establish(const Key& key, std::shared_ptr<Completion<Value>> completion) {
    if (!completion->needs_result()) {
        return Status::OK();
    }

    PendingRead read;
    read.key = key;
    read.completion = std::move(completion);
    const uint64 read_seq = next_read_seq_++;

    RaftEffects effects;
    Status status =
            raft_core_.build_read_quorum_check(effects, read.read_index, read.read_term, read.voter_ids, read_seq);
    if (status.fail()) {
        return status;
    }

    const ReplicaID& self = raft_core_.self_id();
    if (std::find(read.voter_ids.begin(), read.voter_ids.end(), self) != read.voter_ids.end()) {
        read.confirmed_voters.insert(self);
    }

    pending_reads_.emplace(read_seq, std::move(read));

    Status send_status = message_dispatcher_.async_send(std::move(effects.messages));
    if (send_status.fail()) {
        LOG_WARN("[ReadTracker] read quorum async_send failed, status:{}", send_status.to_string());
    }
    return Status::OK();
}

void ReadTracker::on_read_quorum_response(const ReplicaID& from, uint64 read_seq, Term response_term) {
    auto it = pending_reads_.find(read_seq);
    if (it == pending_reads_.end()) {
        return;
    }
    auto& read = it->second;
    if (!read.completion->needs_result()) {
        return;
    }
    if (response_term != read.read_term) return;
    if (std::find(read.voter_ids.begin(), read.voter_ids.end(), from) == read.voter_ids.end()) return;
    if (read.confirmed_voters.insert(from).second) {
        ADVISKV_METRICS_COUNTER("storage_replica_read_quorum_vote");
    }
}

void ReadTracker::advance() {
    if (pending_reads_.empty()) return;
    if (checking_) return;
    checking_ = true;
    auto checking_guard = Defer([&] { checking_ = false; });

    const Term current_term = raft_core_.current_term();
    const bool is_leader = raft_core_.is_leader();

    for (auto it = pending_reads_.begin(); it != pending_reads_.end();) {
        auto& read = it->second;
        if (!read.completion->needs_result()) {
            it = pending_reads_.erase(it);
            continue;
        }

        if (!is_leader || current_term != read.read_term) {
            IGNORE_RESULT(read.completion->complete(
                    CompletionResult<Value>::fail(Status::NOT_LEADER("leader or term changed during read"))))
            it = pending_reads_.erase(it);
            continue;
        }

        const int limit = read.voter_ids.size() / 2 + 1;
        if (read.confirmed_voters.size() < limit) {
            ++it;
            continue;
        }
        if (raft_core_.last_applied() < read.read_index) {
            ++it;
            continue;
        }
        Status confirm_status = raft_core_.confirm_read_index(read.read_index, read.read_term);
        if (confirm_status.fail()) {
            IGNORE_RESULT(read.completion->complete(CompletionResult<Value>::fail(confirm_status)))
            it = pending_reads_.erase(it);
            continue;
        }

        Value value;
        Status get_status = state_machine_.get(read.key, value);
        if (get_status.fail() && get_status.code() != StatusCode::KEY_NOT_FOUND) {
            IGNORE_RESULT(read.completion->complete(CompletionResult<Value>::fail(get_status)))
            pending_reads_.erase(it);
            fault_reporter_.report(Status::ERROR("replica fault"));
            return;
        }
        if (get_status.ok()) {
            IGNORE_RESULT(read.completion->complete(CompletionResult<Value>::ok(std::move(value))))
        } else {
            IGNORE_RESULT(read.completion->complete(CompletionResult<Value>::fail(get_status)))
        }
        ADVISKV_METRICS_COUNTER("storage_replica_read_completed");
        it = pending_reads_.erase(it);
    }
}

void ReadTracker::fail_all(const Status& status) {
    for (auto& [seq, read] : pending_reads_) {
        IGNORE_RESULT(read.completion->complete(CompletionResult<Value>::fail(status)))
    }
    pending_reads_.clear();
}

}  // namespace adviskv::storage
