#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/file.h"
#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {

struct SnapshotData;
class SnapshotStore;

class SnapshotFileReader {
public:
    bool is_open() const { return file_.is_open(); }
    const File& file() const { return file_; }
    const SnapshotHeader& header() const { return header_; }


    Status for_each_kv(const KvVisitor& fn) const;
    Status skip_kv_region() const;
    Status for_each_request(const RequestVisitor& fn) const;
    Status read_chunk(uint64 offset, size_t max_bytes, std::string& data, bool& eof) const;

private:
    friend class SnapshotStore;

    explicit SnapshotFileReader(File&& file) : file_(std::move(file)) {}
    Status initialize();

    File file_;
    SnapshotHeader header_;
    int32 kv_count_{0};
    int32 request_count_{0};
};

class SnapshotStore {
public:
    SnapshotStore(std::string snapshot_path, std::string dir_path);

    Status open_reader(std::unique_ptr<SnapshotFileReader>& reader, bool& exists) const;

    Status verify_header(const SnapshotFileReader& reader, LogIndex expected_index, Term expected_term) const;

    Status load_header(SnapshotHeaderPtr& snap) const;

    Status append_snapshot_chunk(const InstallSnapshotParam& param);
    Status finish_snapshot_receive(const InstallSnapshotParam& param);

    Status write_local_snapshot_candidate(const SnapshotData& snapshot, const std::vector<RaftMember>& members,
                                          LogIndex capture_index);
    Status publish_local_snapshot(LogIndex capture_index, bool& published);

private:
    static Status write_snapshot_to_file(const File& file, const SnapshotData& snapshot,
                                         const std::vector<RaftMember>& members);

    static Status publish_tmp_snapshot(const std::string& tmp_path, const std::string& snapshot_path,
                                       const std::string& dir_path);

    std::string snapshot_path_;
    std::string dir_path_;

    mutable std::mutex mutex_;
};

}  // namespace adviskv::storage
