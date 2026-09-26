#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/model/param.h"
#include "storage/replica/completion_manager.h"

namespace adviskv::storage {

struct ReplicaContext;

struct ProposalItem {
    ProposeParam param;
    CompletionManager::Ticket ticket;
};

// 主要是Queue在处理WritesTask的时候调用到Tracker，还有Loop去触发advance
class ProposalTracker {
public:
    ProposalTracker(ReplicaContext& context, CompletionManager& completions);

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

    ReplicaContext& context_;
    CompletionManager& completions_;
    std::unordered_map<std::string, Entry> entries_;
};

}  // namespace adviskv::storage
