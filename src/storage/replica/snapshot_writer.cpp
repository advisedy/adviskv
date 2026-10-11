#include "storage/replica/snapshot_writer.h"

#include <utility>

#include "common/defer.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/pause_point.h"
#include "storage/persist/snapshot_store.h"

namespace adviskv::storage {

SnapshotWriter::SnapshotWriter(SnapshotStore& snapshot_store, DoneCallback on_done)
        : snapshot_store_(snapshot_store), on_done_(std::move(on_done)) {}

SnapshotWriter::~SnapshotWriter() { stop_and_join(); }

bool SnapshotWriter::is_busy() const {
    std::lock_guard lock(mutex_);
    return worker_busy_;
}

bool SnapshotWriter::write_async(std::shared_ptr<const SnapshotData> snapshot, std::vector<RaftMember> members,
                                 LogIndex capture_index, Term capture_term) {
    {
        std::lock_guard lock(mutex_);
        if (stopping_ || worker_busy_) {
            return false;
        }
        worker_busy_ = true;
    }

    if (worker_ && worker_->joinable()) {
        worker_->join();
    }

    worker_ = std::make_unique<std::thread>(
            [this, snapshot = std::move(snapshot), members = std::move(members), capture_index, capture_term]() {
                run_worker(std::move(snapshot), std::move(members), capture_index, capture_term);
            });
    return true;
}

void SnapshotWriter::run_worker(std::shared_ptr<const SnapshotData> snapshot, std::vector<RaftMember> members,
                                LogIndex capture_index, Term capture_term) {
    auto worker_finish = Defer([this]() {
        std::lock_guard lock(mutex_);
        worker_busy_ = false;
        worker_done_cv_.notify_all();
    });

    ADVISKV_METRICS_COUNTER("storage_replica_take_snapshot_request");
    ADVISKV_METRICS_TIMER("storage_replica_take_snapshot");

    Status status = snapshot_store_.write_local_snapshot_candidate(*snapshot, members, capture_index);
    if (status.fail()) {
        LOG_WARN("[SnapshotWriter] write local snapshot candidate failed, capture_index:{}, status:{}", capture_index,
                 status.to_string());
        on_done_(capture_index, capture_term, std::move(status));
        return;
    }

    testhook::pause_point("snapshot_worker.candidate_written");
    on_done_(capture_index, capture_term, Status::OK());
}

void SnapshotWriter::stop_and_join() {
    std::unique_ptr<std::thread> corpse;
    {
        std::unique_lock lock(mutex_);
        stopping_ = true;
        worker_done_cv_.wait(lock, [this] { return !worker_busy_; });
        corpse = std::move(worker_);
        worker_.reset();
    }
    if (corpse && corpse->joinable()) {
        corpse->join();
    }
}

}
