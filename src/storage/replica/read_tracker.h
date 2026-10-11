#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/replica/completion_manager.h"
#include "storage/replica/fault_reporter.h"

namespace adviskv::storage {

class RaftCore;
class StateMachine;
class ReplicaMessageDispatcher;

struct ReadRequestState {
    Key key;
    CompletionManager::Ticket ticket{0};
    LogIndex read_index{0};
    Term read_term{0};

    uint64 read_seq{0};
    Term read_quorum_term{0};

    std::vector<ReplicaID> voter_ids;
    std::unordered_set<ReplicaID, ReplicaIDHash> confirmed_voters;
    int confirmed_count{1};

    uint64 membership_epoch{0};

    std::chrono::steady_clock::time_point deadline;

    bool done{false};
    Status status{Status::OK()};
    Value value;
};

bool read_quorum_response_counts(ReadRequestState& read, const ReplicaID& from, uint64 response_seq,
                                 Term response_term);

class ReadTracker {
public:
    static constexpr Milliseconds kReadTimeout{2000};
    static constexpr Milliseconds kWaitGrace{500};

    ReadTracker(RaftCore& raft_core, StateMachine& state_machine, ReplicaMessageDispatcher& message_dispatcher,
                CompletionManager& completions, FaultReporter fault_reporter);

    DISALLOW_COPY_AND_ASSIGN(ReadTracker)

    std::shared_ptr<ReadRequestState> new_request(const Key& key, CompletionManager::Ticket ticket);

    Status establish(const std::shared_ptr<ReadRequestState>& state);

    void on_read_quorum_response(const ReplicaID& from, uint64 read_seq, Term response_term);

    void advance();

    void fail_all(const Status& status);

    void note_membership_changed() { ++membership_epoch_; }

    static constexpr Milliseconds wait_deadline() { return kReadTimeout + kWaitGrace; }

private:
    RaftCore& raft_core_;
    StateMachine& state_machine_;
    ReplicaMessageDispatcher& message_dispatcher_;
    CompletionManager& completions_;
    FaultReporter fault_reporter_;

    std::unordered_map<uint64, std::shared_ptr<ReadRequestState>> pending_reads_;
    uint64 next_read_seq_{1};
    uint64_t membership_epoch_{1};
    bool checking_{false};
};

}
