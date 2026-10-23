#include "storage/replica/proposal_tracker.h"

#include <utility>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/raft_step.h"

namespace adviskv::storage {

ProposalTracker::ProposalTracker(RaftCore& raft_core, StateMachine& state_machine, RaftEffectExecutor& effect_executor)
        : raft_core_(raft_core), state_machine_(state_machine), effect_executor_(effect_executor) {}

Status ProposalTracker::submit_batch(std::vector<QueuedCall<ProposeCall>> calls) {
    std::vector<ProposeParam> params;
    std::vector<std::string> candidate_ids;
    params.reserve(calls.size());
    candidate_ids.reserve(calls.size());

    for (const QueuedCall<ProposeCall>& call : calls) {
        auto* write = std::get_if<WriteProposal>(&call.request.param.payload);
        if (write == nullptr || write->request_id.empty()) {
            finish_call(call, Status::INVALID_ARGUMENT("request_id is required for every write proposal"));
            continue;
        }

        uint64_t write_hash = RequestRecord::request_payload_hash(*write);

        ADVISKV_METRICS_COUNTER("storage_replica_request_record_check_request");
        RequestRecord record;
        Status find_status;
        {
            ADVISKV_METRICS_TIMER("storage_replica_request_record_check");
            ADVISKV_METRICS_TIMER("storage_replica_request_record_lookup");
            find_status = state_machine_.find_request(write->request_id, record);
        }
        if (find_status.ok()) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_record_hit");
            Status status = record.payload_hash == write_hash
                                    ? record.apply_status
                                    : Status::INVALID_ARGUMENT("request_id was reused with a different write");
            finish_call(call, status);
            continue;
        }
        if (find_status.code() != StatusCode::KEY_NOT_FOUND) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_record_error");
            finish_call(call, find_status);
            continue;
        }
        ADVISKV_METRICS_COUNTER("storage_replica_request_record_miss");

        auto tracked = entries_.find(write->request_id);
        if (tracked != entries_.end()) {
            if (tracked->second.payload_hash != write_hash) {
                finish_call(call, Status::INVALID_ARGUMENT("request_id was reused with a different write"));
            } else if (tracked->second.committed && write->op != WriteOpType::CAS) {
                finish_call(call, Status::OK());
                ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
            } else {
                // 同一个 request_id 的等待者聚合到同一个 Entry 上
                tracked->second.completions.push_back(call.completion);
                ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
            }
            continue;
        }

        Entry entry;
        entry.op = write->op;
        entry.payload_hash = write_hash;
        entry.completions.push_back(call.completion);
        if (auto* logged = raft_core_.find_request(write->request_id)) {
            uint64_t logged_hash = RequestRecord::request_payload_hash(*logged);
            if (logged_hash != write_hash) {
                finish_call(call, Status::INVALID_ARGUMENT("request_id was reused with a different write"));
                continue;
            }
            if (logged->index <= 0) {
                finish_call(call, Status::ERROR("invalid existing proposal log index"));
                continue;
            }
            entry.index = logged->index;
        }
        if (entry.index > 0) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
        } else {
            params.push_back(call.request.param);
            candidate_ids.push_back(write->request_id);
        }
        entries_.emplace(write->request_id, std::move(entry));
    }

    Status status = propose_new(params, candidate_ids);
    return status;
}

Status ProposalTracker::propose_new(const std::vector<ProposeParam>& params,
                                    const std::vector<std::string>& request_ids) {
    if (params.empty()) {
        return Status::OK();
    }

    std::vector<std::pair<Status, LogIndex>> results;
    Status status = effect_executor_.execute([&](RaftEffects& effects) {
        results = raft_core_.propose_batch(params, effects);
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
    const LogIndex commit_index = raft_core_.commit_index();
    const LogIndex last_applied = raft_core_.last_applied();
    const bool is_leader = raft_core_.is_leader();

    for (auto it = entries_.begin(); it != entries_.end();) {
        Entry& entry = it->second;
        if (!entry.committed && entry.index > 0 && entry.index <= commit_index) {
            entry.committed = true;
            if (entry.op != WriteOpType::CAS) {
                finish_completions(entry, Status::OK(), true);
            }
        }
        if (entry.committed && entry.index > 0 && entry.index <= last_applied) {
            if (entry.op == WriteOpType::CAS) {
                RequestRecord record;
                Status status = state_machine_.find_request(it->first, record);
                if (status.ok()) {
                    status = record.payload_hash == entry.payload_hash
                                     ? record.apply_status
                                     : Status::INVALID_ARGUMENT("request_id was reused with a different write");
                }
                finish_completions(entry, status, true);
            }
            it = entries_.erase(it);
        } else {
            ++it;
        }
    }
    if (!is_leader) {
        fail_all(Status::NOT_LEADER("leader changed before proposal committed"));
    }
}

void ProposalTracker::fail_all(const Status& status) {
    for (auto& [_, entry] : entries_) {
        finish_completions(entry, status);
    }
    entries_.clear();
}

void ProposalTracker::finish_call(const QueuedCall<ProposeCall>& call, const Status& status) {
    if (!call.completion) {
        return;
    }
    if (status.ok()) {
        IGNORE_RESULT(call.completion->complete(CompletionResult<void>::ok()))
    } else {
        IGNORE_RESULT(call.completion->complete(CompletionResult<void>::fail(status)))
    }
}

void ProposalTracker::finish_completions(Entry& entry, const Status& status, bool count_timeout) {
    for (auto& completion : entry.completions) {
        if (!completion) continue;
        CompleteStatus cs;
        if (status.ok()) {
            cs = completion->complete(CompletionResult<void>::ok());
        } else {
            cs = completion->complete(CompletionResult<void>::fail(status));
        }
        if (cs == CompleteStatus::TimedOut && count_timeout) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_commit_after_timeout");
        }
    }
    entry.completions.clear();
}

void ProposalTracker::fail_request(const std::string& request_id, const Status& status) {
    auto it = entries_.find(request_id);
    if (it == entries_.end()) {
        return;
    }
    finish_completions(it->second, status);
    entries_.erase(it);
}

}  // namespace adviskv::storage
