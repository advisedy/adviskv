#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/completion.h"
#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/replica/fault_reporter.h"

namespace adviskv::storage {

class RaftCore;
class StateMachine;
class ReplicaMessageDispatcher;

class ReadTracker {
public:
    ReadTracker(RaftCore& raft_core, StateMachine& state_machine, ReplicaMessageDispatcher& message_dispatcher,
                FaultReporter fault_reporter);

    DISALLOW_COPY_AND_ASSIGN(ReadTracker)

    Status establish(const Key& key, std::shared_ptr<Completion<Value>> completion);

    void on_read_quorum_response(const ReplicaID& from, uint64 read_seq, Term response_term);

    void advance();

    void fail_all(const Status& status);

private:
    struct PendingRead {
        Key key;
        std::shared_ptr<Completion<Value>> completion;
        LogIndex read_index{0};
        Term read_term{0};
        std::vector<ReplicaID> voter_ids;
        std::unordered_set<ReplicaID, ReplicaIDHash> confirmed_voters;
    };

    RaftCore& raft_core_;
    StateMachine& state_machine_;
    ReplicaMessageDispatcher& message_dispatcher_;
    FaultReporter fault_reporter_;

    std::unordered_map<uint64, PendingRead> pending_reads_;
    uint64 next_read_seq_{1};
    bool checking_{false};
};

}  // namespace adviskv::storage
