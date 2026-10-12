#pragma once

#include <cstdint>
#include <memory>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/fault_reporter.h"
#include "storage/replica/snapshot_writer.h"

namespace adviskv::storage {

class RaftEffectExecutor;
struct SnapshotDoneEvent;

class SnapshotTracker {
public:
    SnapshotTracker(ReplicaID replica_id, RaftCore& raft_core, StateMachine& state_machine, PersistEngine& persist,
                    RaftEffectExecutor& effect_executor, FaultReporter fault_reporter, int32_t raft_do_snapshot_limit,
                    SnapshotWriter::DoneCallback on_snapshot_written);

    DISALLOW_COPY_AND_ASSIGN(SnapshotTracker)

    void advance();

    Status on_snapshot_done(const SnapshotDoneEvent& event);

    Status on_install_snapshot_chunk(const InstallSnapshotParam& param);

    void stop_writer() { snapshot_writer_.stop_and_join(); }

private:
    struct ReceivingSnapshot {
        ReplicaID from_replica_id;
        Term term{0};
        LogIndex snapshot_index{0};
        uint64 next_offset{0};
    };

    Status check_receiving_session(const InstallSnapshotParam& param) const;
    Status finish_install_snapshot(const InstallSnapshotParam& param);

    RaftCore& raft_core_;
    StateMachine& state_machine_;
    PersistEngine& persist_;
    RaftEffectExecutor& effect_executor_;
    FaultReporter fault_reporter_;
    ReplicaID replica_id_;

    SnapshotWriter snapshot_writer_;
    int32_t raft_do_snapshot_limit_{0};

    Optional<ReceivingSnapshot> receiving_session_;
};

}
