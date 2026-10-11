#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/persist/snapshot_store.h"

namespace adviskv::storage {

class SnapshotWriter {
public:
    using DoneCallback = std::function<void(LogIndex capture_index, Term capture_term, Status status)>;

    SnapshotWriter(SnapshotStore& snapshot_store, DoneCallback on_done);
    ~SnapshotWriter();

    DISALLOW_COPY_AND_ASSIGN(SnapshotWriter)

    bool write_async(std::shared_ptr<const SnapshotData> snapshot, std::vector<RaftMember> members,
                     LogIndex capture_index, Term capture_term);

    bool is_busy() const;

    void stop_and_join();

private:
    void run_worker(std::shared_ptr<const SnapshotData> snapshot, std::vector<RaftMember> members,
                    LogIndex capture_index, Term capture_term);

    SnapshotStore& snapshot_store_;
    DoneCallback on_done_;

    mutable std::mutex mutex_;
    std::condition_variable worker_done_cv_;
    bool worker_busy_{false};
    bool stopping_{false};
    std::unique_ptr<std::thread> worker_;
};

}
