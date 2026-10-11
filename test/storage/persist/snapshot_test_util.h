#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/model/model.h"
#include "storage/persist/snapshot_store.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::test {

inline adviskv::Status write_state_machine_snapshot(storage::SnapshotStore& store,
                                                    const storage::KvStateMachine& state_machine,
                                                    std::vector<RaftMember> members = {}) {
    storage::SnapshotData snapshot;
    Status status = state_machine.capture_snapshot(snapshot);
    if (status.fail()) {
        return status;
    }
    status = store.write_local_snapshot_candidate(snapshot, members, snapshot.header.apply_index);
    if (status.fail()) {
        return status;
    }
    bool published = false;
    status = store.publish_local_snapshot(snapshot.header.apply_index, published);
    if (status.ok() && !published) {
        return Status::ERROR("snapshot publish rejected as stale");
    }
    return status;
}

inline adviskv::Status for_each_snapshot_kv(storage::SnapshotStore& store,
                                            const storage::KvVisitor& visitor) {
    std::unique_ptr<storage::SnapshotFileReader> reader;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(store.open_reader(reader, exists))
    if (!exists || !reader) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(reader->load(nullptr))
    return reader->for_each_kv(visitor);
}

inline adviskv::Status for_each_snapshot_request(storage::SnapshotStore& store,
                                                 const storage::RequestVisitor& visitor) {
    std::unique_ptr<storage::SnapshotFileReader> reader;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(store.open_reader(reader, exists))
    if (!exists || !reader) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(reader->load(nullptr))
    RETURN_IF_INVALID_STATUS(reader->skip_kv_region())
    return reader->for_each_request(visitor);
}

}
