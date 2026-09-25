#include "storage/replica/replica_loop.h"

#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

namespace {

constexpr size_t K_MAX_PROPOSAL_BATCH_SIZE = 64;
constexpr auto K_PROPOSAL_BATCH_DELAY = Microseconds(200);

}  // namespace

ReplicaLoop::ReplicaLoop(ReplicaContext& context) : context_(context) {}

ReplicaLoop::~ReplicaLoop() { stop(); }

void ReplicaLoop::start() {
    std::lock_guard lock(mutex_);
    if (running_) {
        return;
    }
    running_ = true;
    runner_.start();
    submit_queue_.start(K_PROPOSAL_BATCH_DELAY, K_MAX_PROPOSAL_BATCH_SIZE,
                        [this](std::vector<SubmitQueueItem> items) { on_submit_queue_ready(std::move(items)); });
}

void ReplicaLoop::stop() {
    std::deque<SubmitQueueItem> queued_submissions;
    {
        std::lock_guard lock(mutex_);
        running_ = false;
    }
    queued_submissions = submit_queue_.stop();
    runner_.stop();

    Status stopped = Status::ERROR("raft loop is stopped");
    for (auto& item : queued_submissions) {
        fail_submit_queue_item(item, stopped);
    }
    fail_all_inflight_proposals(stopped);
}

void ReplicaLoop::async_submit(Event event) {
    bool res = std::visit(
            [this](auto concrete_event) -> bool {
                return enqueue([this, event = std::move(concrete_event)]() {
                    Status status = handle_event(event);
                    if (status.fail()) {
                        LOG_WARN("async raft event failed, status:{}", status.to_string());
                    }
                });
            },
            std::move(event));
    if (!res) {
        LOG_WARN("drop event because raft loop is not running");
    }
}

void ReplicaLoop::sync_submit(Call call) {
    std::visit(
            [this](auto* concrete_call) {
                if (concrete_call == nullptr) {
                    LOG_WARN("drop null raft loop call");
                    return;
                }

                using T = std::remove_pointer_t<std::decay_t<decltype(concrete_call)>>;
                if constexpr (std::is_same_v<T, ProposeCall> || std::is_same_v<T, AddMemberCall> ||
                              std::is_same_v<T, RemoveMemberCall>) {
                    // 这里是需要放到Batch里去处理的，不能直接在loop里处理
                    concrete_call->status = handle_call(*concrete_call);
                } else {
                    // 创建waiter，把这个task给enqueue
                    Status status = call_in_loop_task([this, concrete_call]() {
                        concrete_call->status = handle_call(*concrete_call);
                        return Status::OK();
                    });
                    if (status.fail()) {
                        concrete_call->status = status;
                    }
                }
            },
            call);
}

// 正常的创建Waiter，enqueue一个task，task执行完之后调用complete_loop_submit来唤醒waiter
Status ReplicaLoop::call_in_loop_task(TaskFunc&& task) {
    if (!task) {
        return Status::INVALID_ARGUMENT("empty raft loop task");
    }

    auto waiter = std::make_shared<Waiter>();
    bool res = enqueue([task = std::move(task), waiter]() {
        Status status = task();
        complete_loop_submit(waiter, status);
    });

    if (!res) {
        return Status::ERROR("raft loop is not running");
    }

    std::unique_lock lock(waiter->mutex);
    waiter->cv.wait(lock, [&]() { return waiter->done; });
    return waiter->status;
}

Status ReplicaLoop::enqueue_config_change_and_wait(RaftStepFunc&& step) {
    if (!step) {
        return Status::INVALID_ARGUMENT("empty config change step");
    }

    auto waiter = std::make_shared<Waiter>();
    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return Status::ERROR("raft loop is not running");
        }
    }
    if (!submit_queue_.push(ConfigChangeItem{std::move(step), waiter})) {
        return Status::ERROR("raft loop is not running");
    }
    std::unique_lock lock(waiter->mutex);
    waiter->cv.wait(lock, [&]() { return waiter->done; });
    return waiter->status;
}

Status ReplicaLoop::enqueue_proposal_and_wait(ProposeParam param, Milliseconds timeout) {
    auto waiter = std::make_shared<TimeoutWaiter>();

    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return Status::ERROR("raft loop is not running");
        }
    }

    if (!submit_queue_.push(ProposalBatchItem{std::move(param), waiter})) {
        return Status::ERROR("raft loop is not running");
    }

    std::unique_lock lock(waiter->mutex);
    if (!waiter->cv.wait_for(lock, timeout, [&]() { return waiter->done; })) {
        waiter->cancelled = true;
        return Status::NOT_YET_COMMIT("proposal is not committed before timeout");
    }

    return waiter->status;
}

Status ReplicaLoop::handle_event(const TickEvent& event) {
    UNUSED(event);
    Status status = run_step([this](RaftEffects& effects) {
        context_.raft_core.tick(effects);
        return Status::OK();
    });
    resolve_inflight_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const AppendResponseEvent& event) {
    Status status = run_step([this, &event](RaftEffects& effects) {
        return context_.raft_core.handle_append_response(event.from, event.sent_param, event.result, effects);
    });
    resolve_inflight_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const VoteResponseEvent& event) {
    Status status = run_step([this, &event](RaftEffects& effects) {
        context_.raft_core.handle_vote_response(event.from, event.result, effects);
        return Status::OK();
    });
    resolve_inflight_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const PreVoteResponseEvent& event) {
    Status status = run_step([this, &event](RaftEffects& effects) {
        context_.raft_core.handle_pre_vote_response(event.from, event.result, effects);
        return Status::OK();
    });
    resolve_inflight_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const SnapshotResponseEvent& event) {
    Status status = run_step([this, &event](RaftEffects& effects) {
        context_.raft_core.handle_install_snapshot_response(event.from, event.sent_param, event.result, effects);
        return Status::OK();
    });
    resolve_inflight_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const AppendSendFailedEvent& event) {
    Status status = run_step([this, &event](RaftEffects&) {
        context_.raft_core.handle_append_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
    resolve_inflight_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const SnapshotSendFailedEvent& event) {
    Status status = run_step([this, &event](RaftEffects&) {
        context_.raft_core.handle_install_snapshot_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
    resolve_inflight_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const PublishSnapshotEvent& event) {
    {
        std::lock_guard lock(context_.raft_core_mutex);
        if (event.snapshot_index <= context_.raft_core.snapshot_index()) {
            return Status::OK();
        }
        Status status = context_.raft_core.truncate_log(event.snapshot_index);
        if (status.fail()) {
            LOG_WARN(
                    "[ReplicaLoop] handle publish snapshot failed, "
                    "snapshot_index:{}, status:{}",
                    event.snapshot_index, status.to_string());
            context_.enter_faulted();
            return status;
        }
    }
    resolve_inflight_proposals();
    return Status::OK();
}

Status ReplicaLoop::handle_call(RequestVoteCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        LOG_DEBUG(
                "[ReplicaLoop] handle RequestVote, replica_id:{}, from:{}, term:{}, last_log_index:{}, "
                "last_log_term:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.param.last_log_index, call.param.last_log_term);
        context_.raft_core.handle_request_vote(call.param, call.result, effects);
        LOG_DEBUG(
                "[ReplicaLoop] handle RequestVote finished, replica_id:{}, from:{}, request_term:{}, "
                "response_term:{}, vote_granted:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.result.term, call.result.vote_granted);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(PreVoteCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        LOG_DEBUG(
                "[ReplicaLoop] handle PreVote, replica_id:{}, from:{}, term:{}, last_log_index:{}, "
                "last_log_term:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.param.last_log_index, call.param.last_log_term);
        context_.raft_core.handle_pre_vote(call.param, call.result, effects);
        LOG_DEBUG(
                "[ReplicaLoop] handle PreVote finished, replica_id:{}, from:{}, request_term:{}, "
                "response_term:{}, vote_granted:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.result.term, call.result.vote_granted);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(AppendEntriesCall& call) {
    ADVISKV_METRICS_TIMER("storage_replica_append_entries_raft_step_execute");
    return run_step([this, &call](RaftEffects& effects) {
        context_.raft_core.handle_append_entries(call.param, call.result, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(BuildReadIndexCall& call) {
    std::lock_guard lock(context_.raft_core_mutex);
    return context_.raft_core.build_append_entries_for_read(call.effects, call.read_index, call.read_term);
}

Status ReplicaLoop::handle_call(PrepareInstallSnapshotCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        return context_.raft_core.prepare_install_snapshot(call.param, effects);
    });
}

Status ReplicaLoop::handle_call(CommitInstallSnapshotCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        context_.raft_core.commit_install_snapshot(call.context, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(AppendResponseCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(SnapshotResponseCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(SnapshotSendFailedCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(ProposeCall& call) {
    return enqueue_proposal_and_wait(std::move(call.param), call.timeout);
}

Status ReplicaLoop::handle_call(AddMemberCall& call) {
    return enqueue_config_change_and_wait([this, member = call.member](RaftEffects& effects) {
        return context_.raft_core.ensure_add_learner(member, effects);
    });
}

Status ReplicaLoop::handle_call(RemoveMemberCall& call) {
    return enqueue_config_change_and_wait([this, replica_id = call.replica_id](RaftEffects& effects) {
        return context_.raft_core.ensure_remove_member(replica_id, effects);
    });
}

Status ReplicaLoop::run_step(RaftStepFunc&& step) {
    ADVISKV_METRICS_TIMER("storage_replica_raft_loop_step");
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_request");

    RaftEffects effects;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        Status status = step(effects);
        RETURN_IF_INVALID_STATUS(persist_raft_effects(effects))
        RETURN_IF_INVALID_STATUS(status)
    }
    Status send_status = context_.message_dispatcher.async_send(std::move(effects.messages));
    if (send_status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_failure");
        return send_status;
    }
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_success");
    return Status::OK();
}

Status ReplicaLoop::persist_raft_effects(const RaftEffects& effects) {
    if (effects.entries_to_rewrite.has_value() && !effects.entries_to_append.empty()) {
        return Status::INVALID_ARGUMENT("cannot both rewrite and append");
    }

    if (effects.hard_state.has_value()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(context_.persist.save_raft_meta(*effects.hard_state)))
    }

    if (effects.entries_to_rewrite.has_value()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(context_.persist.rewrite_wal(*effects.entries_to_rewrite)))
    }

    if (!effects.entries_to_append.empty()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(context_.persist.append_wal_batch(effects.entries_to_append)))
    }

    return Status::OK();
}
// 只有在runner内部访问，目前并没有并发方面的危险
void ReplicaLoop::resolve_inflight_proposals() {
    LogIndex commit_index{0};
    LogIndex last_applied{0};
    {
        std::lock_guard lock(context_.raft_core_mutex);
        commit_index = context_.raft_core.commit_index();
        last_applied = context_.raft_core.last_applied();
    }

    for (auto it = inflight_.begin(); it != inflight_.end();) {
        InflightProposal& proposal = it->second;
        if (!proposal.committed && proposal.index > 0 && proposal.index <= commit_index) {
            proposal.committed = true;
            if (proposal.proposal.op != WriteOpType::CAS) {
                for (const auto& waiter : proposal.waiters) {
                    if (!complete_proposal_commit(waiter, Status::OK())) {
                        ADVISKV_METRICS_COUNTER("storage_replica_request_commit_after_timeout");
                    }
                }
            }
        }

        if (proposal.committed && proposal.index > 0 && proposal.index <= last_applied) {
            if (proposal.proposal.op == WriteOpType::CAS) {
                RequestRecord record;
                Status status;
                {
                    std::lock_guard lock(context_.state_machine_mutex);
                    status = context_.state_machine.find_request(proposal.proposal.request_id, record);
                }
                if (status.ok()) {
                    status = record.matches(proposal.proposal)
                                     ? record.apply_status
                                     : Status::INVALID_ARGUMENT("request_id was reused with a different write");
                }
                for (const auto& waiter : proposal.waiters) {
                    if (!complete_proposal_commit(waiter, status)) {
                        ADVISKV_METRICS_COUNTER("storage_replica_request_commit_after_timeout");
                    }
                }
            }
            it = inflight_.erase(it);
        } else {
            ++it;
        }
    }

    bool is_leader;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        is_leader = context_.raft_core.is_leader();
    }
    if (!is_leader) {
        fail_all_inflight_proposals(Status::NOT_LEADER("leader changed before proposal committed"));
    }
}

void ReplicaLoop::fail_all_inflight_proposals(const Status& status) {
    for (auto& [_, proposal] : inflight_) {
        for (const auto& waiter : proposal.waiters) {
            complete_proposal_commit(waiter, status);
        }
    }
    inflight_.clear();
}

void ReplicaLoop::on_submit_queue_ready(std::vector<SubmitQueueItem> items) {
    if (items.empty()) {
        return;
    }
    auto queued_items = std::make_shared<std::vector<SubmitQueueItem>>(std::move(items));
    if (!enqueue([this, queued_items]() mutable { drain_submit_queue(std::move(*queued_items)); })) {
        Status stopped = Status::ERROR("raft loop is stopped");
        for (auto& item : *queued_items) {
            fail_submit_queue_item(item, stopped);
        }
    }
}

void ReplicaLoop::drain_submit_queue(std::vector<SubmitQueueItem> items) {
    bool stopped = false;
    {
        std::lock_guard lock(mutex_);
        stopped = !running_;
    }
    if (stopped) {
        Status status = Status::ERROR("raft loop is stopped");
        for (auto& item : items) {
            fail_submit_queue_item(item, status);
        }
        return;
    }

    std::vector<ProposalBatchItem> pending_write_items;
    pending_write_items.reserve(items.size());
    for (size_t i = 0; i < items.size(); i++) {
        if (auto* write_item = std::get_if<ProposalBatchItem>(&items[i])) {
            pending_write_items.push_back(std::move(*write_item));
            continue;
        }

        if (Status status = propose_batch_from_queue(pending_write_items); status.fail()) {
            fail_submit_queue_items(items, i, status);
            return;
        }

        ConfigChangeItem& config_item = std::get<ConfigChangeItem>(items[i]);
        Status status = run_step(std::move(config_item.step));
        resolve_inflight_proposals();
        complete_loop_submit(config_item.waiter, status);
        if (status.fail()) {
            fail_submit_queue_items(items, i + 1, status);
            return;
        }
    }

    IGNORE_RESULT(propose_batch_from_queue(pending_write_items))
}

Status ReplicaLoop::propose_batch_from_queue(std::vector<ProposalBatchItem>& active_items) {
    if (active_items.empty()) {
        return Status::OK();
    }

    // 代码逻辑懒得拆开了，直接一口气在这里做完了，也直观一点，不想再多helper了
    // 首先是把active_items里面request_id之前就遇到过的给筛选出来，
    // 然后对于第一次遇见的跑一次raft_core.propose_batch，更新inflight里面的index，
    // 这样就可以在resolve里面和commit_index比较大小，从而知道结果

    std::vector<size_t> candidate_item_indexs;

    // 先把里面request_id已经遇到过的筛选出来
    for (size_t i = 0; i < active_items.size(); ++i) {
        ProposalBatchItem& item = active_items[i];
        const auto* write = std::get_if<WriteProposal>(&item.param.payload);
        if (write == nullptr || write->request_id.empty()) {
            complete_proposal_commit(item.waiter,
                                     Status::INVALID_ARGUMENT("request_id is required for every write proposal"));
            continue;
        }

        ADVISKV_METRICS_COUNTER("storage_replica_request_record_check_request");
        RequestRecord committed_record;
        Status find_status;
        {
            ADVISKV_METRICS_TIMER("storage_replica_request_record_check");
            std::lock_guard lock(context_.state_machine_mutex);
            {
                ADVISKV_METRICS_TIMER("storage_replica_request_record_lookup");
                find_status = context_.state_machine.find_request(write->request_id, committed_record);
            }
        }
        if (find_status.ok()) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_record_hit");
            Status status = committed_record.matches(*write)
                                    ? committed_record.apply_status
                                    : Status::INVALID_ARGUMENT("request_id was reused with a different write");
            complete_proposal_commit(item.waiter, status);

            continue;
        } else if (find_status.code() != StatusCode::KEY_NOT_FOUND) {
            ADVISKV_METRICS_COUNTER("storage_replica_request_record_error");
            complete_proposal_commit(item.waiter, find_status);
            continue;
        }
        ADVISKV_METRICS_COUNTER("storage_replica_request_record_miss");
        // 说明在状态机里面没有找到request_id对应的操作
        // 这里分成两个情况，batch里有request_id重复的，或者没有重复的
        // 所以如果inflight和raftlog里面都没有的话，就代表是第一次见的新的写操作
        // 否则说明这个写操作之前出现过，这里有可能是因为当前 leader
        // 之前执行过那个写操作还没有apply，也有可能是切主前的还没apply的 log
        // 留下来的（后者就需要查询raftlog了，因为inflight只是记录本机的）
        auto inflight_it = inflight_.find(write->request_id);
        if (inflight_it == inflight_.end()) {
            std::lock_guard lock(context_.raft_core_mutex);
            if (const LogEntry* entry = context_.raft_core.find_request(write->request_id)) {
                InflightProposal proposal;
                proposal.proposal = WriteProposal{entry->op_type, entry->key, entry->value, entry->request_id, entry->expected};
                proposal.index = entry->index;
                proposal.committed = entry->index > 0 && entry->index <= context_.raft_core.commit_index();
                inflight_it = inflight_.emplace(write->request_id, std::move(proposal)).first;
            }
        }
        if (inflight_it != inflight_.end()) {
            InflightProposal& proposal = inflight_it->second;
            if ((*write) != proposal.proposal) {
                complete_proposal_commit(item.waiter,
                                         Status::INVALID_ARGUMENT("request_id was reused with a different write"));
            } else if (proposal.committed && write->op != WriteOpType::CAS) {
                ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
                complete_proposal_commit(item.waiter, Status::OK());
            } else {
                ADVISKV_METRICS_COUNTER("storage_replica_request_pending_deduplicated");
                proposal.waiters.push_back(item.waiter);
            }
            continue;
        }

        // 说明是第一次看到该 request_id
        InflightProposal proposal;
        proposal.proposal = *write;
        proposal.waiters.push_back(item.waiter);
        inflight_.emplace(write->request_id, std::move(proposal));
        candidate_item_indexs.push_back(i);
    }

    std::vector<ProposeParam> params;  // 代表真正需要走进Raft的proposal的params
    params.reserve(candidate_item_indexs.size());
    for (size_t item_index : candidate_item_indexs) {
        params.push_back(active_items[item_index].param);
    }

    // 一个lambda，作用是把candidate_item_indexs里
    // 面对应的waitter都设置status，并且inflight里面的都删掉
    auto fail_new_candidates = [&](const Status& status) {
        for (size_t item_index : candidate_item_indexs) {
            ProposalBatchItem& item = active_items[item_index];
            const auto* write = std::get_if<WriteProposal>(&item.param.payload);
            if (write == nullptr) {
                continue;
            }

            if (auto inflight_it = inflight_.find(write->request_id); inflight_it != inflight_.end()) {
                for (const auto& waiter : inflight_it->second.waiters) {
                    IGNORE_RESULT(complete_proposal_commit(waiter, status))
                }
                inflight_.erase(inflight_it);
            }
        }
    };

    if (params.empty()) {
        resolve_inflight_proposals();
        active_items.clear();
        return Status::OK();
    }

    std::vector<std::pair<Status, LogIndex>> results;
    Status step_status = run_step([&](RaftEffects& effects) {
        results = context_.raft_core.propose_batch(params, effects);
        return Status::OK();
    });

    if (step_status.fail()) {
        fail_new_candidates(step_status);
        resolve_inflight_proposals();
        active_items.clear();
        return step_status;
    }

    if (results.size() != candidate_item_indexs.size()) {
        Status status = Status::ERROR("proposal batch result size mismatch");
        fail_new_candidates(status);
        resolve_inflight_proposals();
        active_items.clear();
        return status;
    }

    // 到此，成功执行完了raft_core的propose_batch

    for (size_t candidate_index = 0; candidate_index < candidate_item_indexs.size(); candidate_index++) {
        ProposalBatchItem& item = active_items[candidate_item_indexs[candidate_index]];
        Status status = results[candidate_index].first;
        LogIndex index = results[candidate_index].second;

        const auto* write = std::get_if<WriteProposal>(&item.param.payload);
        if (write == nullptr || write->request_id.empty()) {
            complete_proposal_commit(item.waiter,
                                     Status::INVALID_ARGUMENT("request_id is required for every write proposal"));
            continue;
        }

        auto inflight_it = inflight_.find(write->request_id);
        if (inflight_it == inflight_.end()) {
            complete_proposal_commit(item.waiter, Status::ERROR("inflight proposal is missing"));
            continue;
        }

        InflightProposal& proposal = inflight_it->second;
        if (status.fail()) {
            for (const auto& waiter : proposal.waiters) {
                complete_proposal_commit(waiter, status);
            }
            inflight_.erase(inflight_it);
            continue;
        }
        if (index <= 0) {
            Status invalid_index = Status::ERROR("invalid proposed log index");
            for (const auto& waiter : proposal.waiters) {
                complete_proposal_commit(waiter, invalid_index);
            }
            inflight_.erase(inflight_it);
            continue;
        }

        proposal.index = index;
    }

    resolve_inflight_proposals();
    active_items.clear();
    return Status::OK();
}

void ReplicaLoop::complete_loop_submit(const std::shared_ptr<Waiter>& waiter, const Status& status) {
    {
        std::lock_guard lock(waiter->mutex);
        waiter->status = status;
        waiter->done = true;
    }
    waiter->cv.notify_one();
}

// 返回 true，代表本次操作唤醒了waiter
bool ReplicaLoop::complete_proposal_commit(const std::shared_ptr<TimeoutWaiter>& waiter, const Status& status) {
    {
        std::lock_guard lock(waiter->mutex);
        if (waiter->cancelled || waiter->done) {
            return false;
        }
        waiter->status = status;
        waiter->done = true;
    }
    waiter->cv.notify_one();
    return true;
}

void ReplicaLoop::fail_submit_queue_item(SubmitQueueItem& item, const Status& status) {
    if (auto* write_item = std::get_if<ProposalBatchItem>(&item)) {
        IGNORE_RESULT(complete_proposal_commit(write_item->waiter, status))
        return;
    }

    ConfigChangeItem& config_item = std::get<ConfigChangeItem>(item);
    complete_loop_submit(config_item.waiter, status);
}

void ReplicaLoop::fail_submit_queue_items(std::vector<SubmitQueueItem>& items, size_t start_idx, const Status& status) {
    for (size_t i = start_idx; i < items.size(); i++) {
        fail_submit_queue_item(items[i], status);
    }
}

bool ReplicaLoop::enqueue(QueueTask&& task) {
    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return false;
        }
    }
    return runner_.submit(std::move(task));
}

}  // namespace adviskv::storage
