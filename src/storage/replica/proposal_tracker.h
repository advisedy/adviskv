#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/replica/replica_work.h"

namespace adviskv::storage {

class RaftCore;
class StateMachine;
class RaftEffectExecutor;

class ProposalTracker {
public:
    ProposalTracker(RaftCore& raft_core, StateMachine& state_machine, RaftEffectExecutor& effect_executor);

    Status submit_batch(std::vector<QueuedCall<ProposeCall>> calls);
    void advance();
    void fail_all(const Status& status);

private:
    struct Entry {
        WriteOpType op{WriteOpType::NONE};
        uint64_t payload_hash{0};
        LogIndex index{0};
        bool committed{false};
        std::vector<std::shared_ptr<Completion<void>>> completions;
    };

    Status propose_new(const std::vector<ProposeParam>& params, const std::vector<std::string>& request_ids);
    void finish_call(const QueuedCall<ProposeCall>& call, const Status& status);
    void finish_completions(Entry& entry, const Status& status, bool count_timeout = false);
    void fail_request(const std::string& request_id, const Status& status);

    RaftCore& raft_core_;
    StateMachine& state_machine_;
    RaftEffectExecutor& effect_executor_;
    std::unordered_map<std::string, Entry> entries_;
};

}  // namespace adviskv::storage
