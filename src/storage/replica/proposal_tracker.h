#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/model/param.h"
#include "storage/replica/completion_manager.h"

namespace adviskv::storage {

class RaftCore;
class StateMachine;
class RaftEffectExecutor;

struct ProposalItem {
    ProposeParam param;
    CompletionManager::Ticket ticket;
};

class ProposalTracker {
public:
    ProposalTracker(RaftCore& raft_core, StateMachine& state_machine, RaftEffectExecutor& effect_executor,
                    CompletionManager& completions);

    Status submit_batch(std::vector<ProposalItem> items);
    void advance();
    void fail_all(const Status& status);

private:
    struct Entry {
        WriteProposal proposal;
        LogIndex index{0};
        bool committed{false};
        std::vector<CompletionManager::Ticket> tickets;
    };

    Status propose_new(const std::vector<ProposeParam>& params, const std::vector<std::string>& request_ids);
    void finish_tickets(Entry& entry, const Status& status, bool count_timeout = false);
    void fail_request(const std::string& request_id, const Status& status);

    RaftCore& raft_core_;
    StateMachine& state_machine_;
    RaftEffectExecutor& effect_executor_;
    CompletionManager& completions_;
    std::unordered_map<std::string, Entry> entries_;
};

}
