#include "storage/replica/snapshot_tracker.h"

#include <utility>

#include "common/metrics/metrics.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/raft_step.h"
#include "storage/replica/replica_loop.h"

namespace adviskv::storage {

SnapshotTracker::SnapshotTracker(ReplicaID replica_id, RaftCore& raft_core, StateMachine& state_machine,
                                 PersistEngine& persist, RaftEffectExecutor& effect_executor, FaultReporter fault_reporter,
                                 int32_t raft_do_snapshot_limit, std::function<void()> on_membership_changed,
                                 SnapshotWriter::DoneCallback on_snapshot_written)
        : raft_core_(raft_core),
          state_machine_(state_machine),
          persist_(persist),
          effect_executor_(effect_executor),
          fault_reporter_(std::move(fault_reporter)),
          on_membership_changed_(std::move(on_membership_changed)),
          replica_id_(std::move(replica_id)),
          snapshot_writer_(persist.snapshot(), std::move(on_snapshot_written)),
          raft_do_snapshot_limit_(raft_do_snapshot_limit) {}

void SnapshotTracker::advance() {
    const LogIndex last_applied = raft_core_.last_applied();
    const LogIndex snapshot_index = raft_core_.snapshot_index();
    if (last_applied < snapshot_index || last_applied - snapshot_index < to<LogIndex>(raft_do_snapshot_limit_)) {
        return;
    }
    if (snapshot_writer_.is_busy()) {
        return;
    }

    auto snapshot = std::make_shared<SnapshotData>();
    Status status = state_machine_.capture_snapshot(*snapshot);
    if (status.fail()) {
        LOG_WARN("[SnapshotTracker] capture snapshot failed, replica_id:{}, status:{}", replica_id_.to_string(),
                 status.to_string());
        fault_reporter_.report(Status::ERROR("replica fault"));
        return;
    }

    LOG_INFO(
            "[SnapshotTracker] snapshot captured, replica_id:{}, capture_index:{}, capture_term:{}, kv_count:{}",
            replica_id_.to_string(), snapshot->header.apply_index, snapshot->header.apply_term,
            snapshot->kvs.size());

    const LogIndex capture_index = snapshot->header.apply_index;
    const Term capture_term = snapshot->header.apply_term;
    std::vector<RaftMember> members = raft_core_.raft_members();
    snapshot_writer_.write_async(std::move(snapshot), std::move(members), capture_index, capture_term);
}

Status SnapshotTracker::on_snapshot_done(const SnapshotDoneEvent& event) {
    if (event.status.fail()) {
        LOG_WARN("[SnapshotTracker] snapshot candidate write failed, replica_id:{}, capture_index:{}, status:{}",
                 replica_id_.to_string(), event.capture_index, event.status.to_string());
        fault_reporter_.report(Status::ERROR("replica fault"));
        return event.status;
    }

    const LogIndex current_snapshot_index = raft_core_.snapshot_index();
    if (event.capture_index <= current_snapshot_index) {
        LOG_INFO(
                "[SnapshotTracker] skip stale snapshot done, replica_id:{}, capture_index:{}, current_snapshot_index:{}",
                replica_id_.to_string(), event.capture_index, current_snapshot_index);
        bool stale_published = false;
        Status cleanup_status = fault_reporter_.fail_if(
                persist_.snapshot().publish_local_snapshot(event.capture_index, stale_published));
        UNUSED(stale_published)
        RETURN_IF_INVALID_STATUS(cleanup_status)
        return Status::OK();
    }

    bool published = false;
    Status publish_status = fault_reporter_.fail_if(
            persist_.snapshot().publish_local_snapshot(event.capture_index, published));
    RETURN_IF_INVALID_STATUS(publish_status)

    SnapshotHeader header;
    header.apply_index = event.capture_index;
    header.apply_term = event.capture_term;
    header.members = {};

    Status exec_status = effect_executor_.execute([this, &header](RaftEffects& effects) {
        raft_core_.install_snapshot(header, effects);
        return Status::OK();
    });
    if (exec_status.fail()) {
        LOG_WARN("[SnapshotTracker] install local snapshot failed, replica_id:{}, status:{}",
                 replica_id_.to_string(), exec_status.to_string());
        fault_reporter_.report(Status::ERROR("replica fault"));
        return exec_status;
    }

    if (on_membership_changed_) {
        on_membership_changed_();
    }

    LOG_INFO("[SnapshotTracker] local snapshot published, replica_id:{}, snapshot_index:{}, snapshot_term:{}",
             replica_id_.to_string(), event.capture_index, event.capture_term);
    return Status::OK();
}

Status SnapshotTracker::check_receiving_session(const InstallSnapshotParam& param) const {
    if (receiving_session_.is_empty()) {
        return Status::ERROR(fmt::format("[SnapshotTracker] replica:{} received snapshot chunk without session, from:{}, "
                                         "snapshot_index:{}, snapshot_term:{}, offset:{}",
                                         param.to_replica_id.to_string(), param.from_replica_id.to_string(),
                                         param.snapshot_index, param.snapshot_term, param.offset));
    }
    const ReceivingSnapshot& session = receiving_session_.value();
    if (session.from_replica_id != param.from_replica_id || session.term != param.term ||
        session.snapshot_index != param.snapshot_index) {
        return Status::ERROR(
                fmt::format("[SnapshotTracker] replica:{} received snapshot chunk from different session, "
                            "chunk:[from:{}, term:{}, snapshot_index:{}], session:[from:{}, term:{}, "
                            "snapshot_index:{}]",
                            param.to_replica_id.to_string(), param.from_replica_id.to_string(), param.term,
                            param.snapshot_index, session.from_replica_id.to_string(), session.term,
                            session.snapshot_index));
    }
    if (session.next_offset != param.offset) {
        return Status::ERROR(fmt::format("[SnapshotTracker] replica:{} received snapshot chunk with unexpected offset, "
                                         "from:{}, snapshot_index:{}, expected_offset:{}, actual_offset:{}",
                                         param.to_replica_id.to_string(), param.from_replica_id.to_string(),
                                         param.snapshot_index, session.next_offset, param.offset));
    }
    return Status::OK();
}

Status SnapshotTracker::on_install_snapshot_chunk(const InstallSnapshotParam& param) {
    if (param.offset == 0) {
        LOG_INFO("[SnapshotTracker] replica:{} start install snapshot from replica:{}, snapshot index:{}, term:{}",
                 param.to_replica_id.to_string(), param.from_replica_id.to_string(), param.snapshot_index,
                 param.snapshot_term);
    }

    LogIndex watermark = 0;
    Status prepare_status = effect_executor_.execute([this, &param, &watermark](RaftEffects& effects) {
        return raft_core_.prepare_install_snapshot(param, effects, watermark);
    });
    RETURN_IF_INVALID_STATUS(prepare_status)

    if (watermark >= param.snapshot_index) {
        receiving_session_.reset();
        return Status::OK();
    }

    if (param.offset == 0) {
        receiving_session_ = ReceivingSnapshot{param.from_replica_id, param.term, param.snapshot_index, 0};
    } else {
        RETURN_IF_INVALID_STATUS(check_receiving_session(param))
    }

    RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(persist_.snapshot().append_snapshot_chunk(param)))

    receiving_session_->next_offset = param.offset + param.data.size();
    if (!param.done) {
        return Status::OK();
    }

    Status finish_status = finish_install_snapshot(param);
    receiving_session_.reset();
    RETURN_IF_INVALID_STATUS(finish_status)

    LOG_INFO("[SnapshotTracker] replica:{} finish install snapshot from replica:{}, snapshot index:{}, term:{}",
             param.to_replica_id.to_string(), param.from_replica_id.to_string(), param.snapshot_index,
             param.snapshot_term);
    return Status::OK();
}

Status SnapshotTracker::finish_install_snapshot(const InstallSnapshotParam& param) {
    RETURN_IF_INVALID_STATUS(
            fault_reporter_.fail_if(persist_.snapshot().finish_snapshot_receive(param)))

    std::unique_ptr<SnapshotFileReader> reader;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(persist_.snapshot().open_reader(reader, exists)))
    if (!exists || !reader) {
        return Status::ERROR("published snapshot is unreadable right after receive");
    }

    SnapshotHeaderPtr snap = std::make_shared<SnapshotHeader>();
    RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(reader->load(snap.get())))

    SnapshotFileReader* reader_ptr = reader.get();
    Status restore_status = fault_reporter_.fail_if(state_machine_.restore(
            snap,
            [reader_ptr](const KvVisitor& visitor) -> Status { return reader_ptr->for_each_kv(visitor); },
            [reader_ptr](const RequestVisitor& visitor) -> Status { return reader_ptr->for_each_request(visitor); }));
    RETURN_IF_INVALID_STATUS(restore_status)

    SnapshotHeader install_header;
    install_header.apply_index = snap->apply_index;
    install_header.apply_term = snap->apply_term;
    install_header.members = snap->members;

    Status status = effect_executor_.execute([this, &install_header](RaftEffects& effects) {
        raft_core_.install_snapshot(install_header, effects);
        return Status::OK();
    });
    if (status.ok() && !install_header.members.empty() && on_membership_changed_) {
        on_membership_changed_();
    }
    return status;
}

}
