#include "storage/replica/proposal_tracker.h"

#include <mutex>
#include <utility>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "storage/raft/core/raft_core.h"
#include "storage/replica/replica.h"
#include "storage/replica/raft_step.h"

namespace adviskv::storage {

ProposalTracker::ProposalTracker(ReplicaContext& context, CompletionManager& completions)
        : context_(context), completions_(completions) {}

Status ProposalTracker::submit_batch(std::vector<ProposalItem> items) {
    std::vector<ProposeParam> params;
    std::vector<std::string> candidate_ids;
    params.reserve(items.size());
    candidate_ids.reserve(items.size());

    for (const ProposalItem& item : items) {
        auto* write = std::get_if<WriteProposal>(&item.param.payload);
        if (write == nullptr || write->request_id.empty()) {
            IGNORE_RESULT(completions_.finish(
                    item.ticket, Status::INVALID_ARGUMENT("request_id is required for every write proposal")))
            continue;
        }

        ADVISKV_METRICS_COUNTER("storage_replica_request_record_check_request");
        RequestRecord record;
        Status find_status;
        {
            ADVISKV_METRICS_TIMER("storage_replica_request_record_check");
            std::lock_guard lock(context_.state_machine_mutex);
            ADVISKV_METRICS_TIMER("storage_replica_request_record_lookup");
            find_status = context_.state_machine.find_request(write->request_id, record);
        }
        if (find_status.ok()) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_record_hit");
            Status status = record.matches(*write)
                                    ? record.apply_status
                                    : Status::INVALID_ARGUMENT("request_id was reused with a different write");
            IGNORE_RESULT(completions_.finish(item.ticket, status))
            continue;
        }
        if (find_status.code() != StatusCode::KEY_NOT_FOUND) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_record_error");
            IGNORE_RESULT(completions_.finish(item.ticket, find_status))
            continue;
        }
        ADVISKV_METRICS_COUNTER("storage_replica_request_record_miss");

        auto tracked = entries_.find(write->request_id);
        if (tracked != entries_.end()) {
            if (tracked->second.proposal != *write) {
                IGNORE_RESULT(completions_.finish(
                        item.ticket, Status::INVALID_ARGUMENT("request_id was reused with a different write")))
            } else if (tracked->second.committed && write->op != WriteOpType::CAS) {
                IGNORE_RESULT(completions_.finish(item.ticket, Status::OK()))
                ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
            } else {
                tracked->second.tickets.push_back(item.ticket);
                ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
            }
            continue;
        }

        Entry entry;
        entry.proposal = *write;
        entry.tickets.push_back(item.ticket);
        {
            std::lock_guard lock(context_.raft_core_mutex);
            if (auto* logged = context_.raft_core.find_request(write->request_id)) {
                WriteProposal logged_write{logged->op_type, logged->key, logged->value,
                                           logged->request_id, logged->expected};
                if (logged_write != *write) {
                    IGNORE_RESULT(completions_.finish(
                            item.ticket, Status::INVALID_ARGUMENT("request_id was reused with a different write")))
                    continue;
                }
                if (logged->index <= 0) {
                    IGNORE_RESULT(completions_.finish(item.ticket, Status::ERROR("invalid existing proposal log index")))
                    continue;
                }
                entry.index = logged->index;
            }
        }
        if (entry.index > 0) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
        } else {
            params.push_back(item.param);
            candidate_ids.push_back(write->request_id);
        }
        entries_.emplace(write->request_id, std::move(entry));
    }

    Status status = propose_new(params, candidate_ids);
    advance();
    return status;
}

Status ProposalTracker::propose_new(const std::vector<ProposeParam>& params,
                                    const std::vector<std::string>& request_ids) {
    if (params.empty()) {
        return Status::OK();
    }

    std::vector<std::pair<Status, LogIndex>> results;
    Status status = run_raft_step(context_, [&](RaftEffects& effects) {
        results = context_.raft_core.propose_batch(params, effects);
        return Status::OK();
    });
    if (status.fail()) {
        for (const std::string& request_id : request_ids) {
            fail_request(request_id, status);
        }
        return status;
    }
    if (results.size() != request_ids.size()) {
        status = Status::ERROR("proposal batch result size mismatch");
        for (const std::string& request_id : request_ids) {
            fail_request(request_id, status);
        }
        return status;
    }
    for (size_t i = 0; i < request_ids.size(); ++i) {
        if (results[i].first.fail()) {
            fail_request(request_ids[i], results[i].first);
        } else if (results[i].second <= 0) {
            fail_request(request_ids[i], Status::ERROR("invalid proposed log index"));
        } else {
            entries_.at(request_ids[i]).index = results[i].second;
        }
    }
    return Status::OK();
}

void ProposalTracker::advance() {
    LogIndex commit_index{0};
    LogIndex last_applied{0};
    bool is_leader{false};
    {
        std::lock_guard lock(context_.raft_core_mutex);
        commit_index = context_.raft_core.commit_index();
        last_applied = context_.raft_core.last_applied();
        is_leader = context_.raft_core.is_leader();
    }

    bool notify_apply{false};
    for (auto it = entries_.begin(); it != entries_.end();) {
        Entry& entry = it->second;
        if (!entry.committed && entry.index > 0 && entry.index <= commit_index) {
            entry.committed = true;
            if (entry.proposal.op == WriteOpType::CAS) {
                notify_apply = true;
            } else {
                finish_tickets(entry, Status::OK(), true);
            }
        }
        if (entry.committed && entry.index > 0 && entry.index <= last_applied) {
            if (entry.proposal.op == WriteOpType::CAS) {
                RequestRecord record;
                Status status;
                {
                    std::lock_guard lock(context_.state_machine_mutex);
                    status = context_.state_machine.find_request(entry.proposal.request_id, record);
                }
                if (status.ok()) {
                    status = record.matches(entry.proposal)
                                     ? record.apply_status
                                     : Status::INVALID_ARGUMENT("request_id was reused with a different write");
                }
                finish_tickets(entry, status, true);
            }
            it = entries_.erase(it);
        } else {
            ++it;
        }
    }
    if (notify_apply && context_.notify_apply) {
        context_.notify_apply();
    }
    if (!is_leader) {
        fail_all(Status::NOT_LEADER("leader changed before proposal committed"));
    }
}

void ProposalTracker::fail_all(const Status& status) {
    for (auto& [_, entry] : entries_) {
        finish_tickets(entry, status);
    }
    entries_.clear();
}

void ProposalTracker::finish_tickets(Entry& entry, const Status& status, bool count_timeout) {
    for (CompletionManager::Ticket ticket : entry.tickets) {
        if (!completions_.finish(ticket, status) && count_timeout) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_commit_after_timeout");
        }
    }
    entry.tickets.clear();
}

void ProposalTracker::fail_request(const std::string& request_id, const Status& status) {
    auto it = entries_.find(request_id);
    if (it == entries_.end()) {
        return;
    }
    finish_tickets(it->second, status);
    entries_.erase(it);
}

}  // namespace adviskv::storage
