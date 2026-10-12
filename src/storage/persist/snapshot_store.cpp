#include "storage/persist/snapshot_store.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/binary/codec.h"
#include "common/constants.h"
#include "common/crc.h"
#include "common/defer.h"
#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/pause_point.h"
#include "common/path.h"
#include "storage/persist/persist_codecs.h"

namespace adviskv::storage {

namespace {

constexpr int32_t K_SNAPSHOT_MAGIC = 0x4B534850;
constexpr size_t K_SNAPSHOT_TRAILER_BYTES = 4 + 4 + 4 + 8 + 4;

struct SnapshotTrailer {
    int32 kv_count{0};
    int32 request_count{0};
    uint32_t body_crc32{0};
    int64 total_len{0};
};

Status read_snapshot_trailer(BinaryReader& reader, SnapshotTrailer& trailer) {
    RETURN_IF_INVALID_STATUS(binary::decode(reader, trailer.kv_count))
    RETURN_IF_INVALID_STATUS(binary::decode(reader, trailer.request_count))
    RETURN_IF_INVALID_STATUS(binary::decode(reader, trailer.body_crc32))
    RETURN_IF_INVALID_STATUS(binary::decode(reader, trailer.total_len))
    int32 tail_magic{0};
    RETURN_IF_INVALID_STATUS(binary::decode(reader, tail_magic))
    if (tail_magic != K_SNAPSHOT_MAGIC) {
        return Status::ERROR("snapshot trailer magic mismatch, not a snapshot file or not fully written");
    }
    if (trailer.kv_count < 0 || trailer.request_count < 0) {
        return Status::ERROR(fmt::format("invalid snapshot counts, kv_count:{}, request_count:{}", trailer.kv_count,
                                         trailer.request_count));
    }
    return Status::OK();
}

Status verify_snapshot_file(const File& file, SnapshotTrailer& trailer) {
    uint64_t file_size{0};
    RETURN_IF_INVALID_STATUS(file.size(file_size))
    if (file_size <= K_SNAPSHOT_TRAILER_BYTES) {
        return Status::ERROR(fmt::format("snapshot file too small to be complete: {}", file_size));
    }

    FileReader reader(file);
    RETURN_IF_INVALID_STATUS(file.seek(file_size - K_SNAPSHOT_TRAILER_BYTES))
    RETURN_IF_INVALID_STATUS(read_snapshot_trailer(reader, trailer))
    if (trailer.total_len != static_cast<int64>(file_size)) {
        return Status::ERROR(
                fmt::format("snapshot total_len mismatch, trailer:{} actual:{}", trailer.total_len, file_size));
    }

    RETURN_IF_INVALID_STATUS(file.seek(0))
    int32 head_magic{0};
    RETURN_IF_INVALID_STATUS(binary::decode(reader, head_magic))
    if (head_magic != K_SNAPSHOT_MAGIC) {
        return Status::ERROR("snapshot head magic mismatch, not a snapshot file");
    }
    uint32_t running_crc = update_crc32(0, reinterpret_cast<const uint8_t*>(&head_magic), sizeof(head_magic));
    size_t done = sizeof(head_magic);
    const size_t body_size = file_size - K_SNAPSHOT_TRAILER_BYTES;
    std::vector<uint8_t> chunk(K_SNAPSHOT_VERIFY_BUFFER_BYTES);
    while (done < body_size) {
        size_t want = std::min(chunk.size(), body_size - done);
        RETURN_IF_INVALID_STATUS(reader.read_raw(chunk.data(), want))
        running_crc = update_crc32(running_crc, chunk.data(), want);
        done += want;
    }
    if (running_crc != trailer.body_crc32) {
        return Status::ERROR("snapshot body crc mismatch, file is corrupted");
    }
    return Status::OK();
}

}  // namespace

SnapshotStore::SnapshotStore(std::string snapshot_path, std::string dir_path)
        : snapshot_path_(std::move(snapshot_path)), dir_path_(std::move(dir_path)) {}

Status SnapshotStore::write_snapshot_to_file(const File& file, const SnapshotData& snapshot,
                                             const std::vector<RaftMember>& members) {
    FileWriter file_writer(file);
    ChecksumWriter checksum(file_writer);

    RETURN_IF_INVALID_STATUS(binary::encode(K_SNAPSHOT_MAGIC, checksum))
    SnapshotHeader header = snapshot.header;
    header.members = members;
    RETURN_IF_INVALID_STATUS(binary::encode(header, checksum))

    int32 kv_count = 0;
    for (const auto& [k, v] : snapshot.kvs) {
        RETURN_IF_INVALID_STATUS(binary::encode(k, checksum))
        RETURN_IF_INVALID_STATUS(binary::encode(v, checksum))
        ++kv_count;
    }

    int32 request_count = 0;
    for (const RequestRecord& record : snapshot.request_records) {
        if (record.request_id.empty()) {
            return Status::INVALID_ARGUMENT("cannot snapshot an empty request_id");
        }
        RETURN_IF_INVALID_STATUS(binary::encode(record, checksum))
        ++request_count;
    }
    ADVISKV_METRICS_COUNTER("storage_persist_snapshot_request_records_written", request_count);

    uint32_t body_crc = checksum.result();
    int64 total_len = static_cast<int64>(checksum.bytes() + K_SNAPSHOT_TRAILER_BYTES);
    RETURN_IF_INVALID_STATUS(binary::encode(kv_count, file_writer))
    RETURN_IF_INVALID_STATUS(binary::encode(request_count, file_writer))
    RETURN_IF_INVALID_STATUS(binary::encode(body_crc, file_writer))
    RETURN_IF_INVALID_STATUS(binary::encode(total_len, file_writer))
    RETURN_IF_INVALID_STATUS(binary::encode(K_SNAPSHOT_MAGIC, file_writer))

    ADVISKV_METRICS_COUNTER("storage_persist_snapshot_bytes_written", total_len);
    return file_writer.sync();
}

Status SnapshotFileReader::initialize() {
    if (!file_.is_open()) {
        return Status::ERROR("snapshot reader is not open");
    }

    SnapshotTrailer trailer;
    RETURN_IF_INVALID_STATUS(verify_snapshot_file(file_, trailer))
    kv_count_ = trailer.kv_count;
    request_count_ = trailer.request_count;


    RETURN_IF_INVALID_STATUS(file_.seek(sizeof(int32)))
    FileReader reader(file_);
    return binary::decode(reader, header_);
}

Status SnapshotFileReader::for_each_kv(const KvVisitor& fn) const {
    if (!file_.is_open()) {
        return Status::ERROR("snapshot reader is not open");
    }
    FileReader reader(file_);
    for (int i = 0; i < kv_count_; i++) {
        Key key;
        Value value;
        RETURN_IF_INVALID_STATUS(binary::decode(reader, key))
        RETURN_IF_INVALID_STATUS(binary::decode(reader, value))
        RETURN_IF_INVALID_STATUS(fn(key, value))
    }
    return Status::OK();
}

Status SnapshotFileReader::skip_kv_region() const {
    if (!file_.is_open()) {
        return Status::ERROR("snapshot reader is not open");
    }
    FileReader reader(file_);
    for (int i = 0; i < kv_count_; i++) {
        for (int f = 0; f < 2; f++) {
            int32 len{0};
            RETURN_IF_INVALID_STATUS(binary::decode(reader, len))
            if (len < 0) {
                return Status::ERROR("invalid kv len while skipping snapshot kv region");
            }
            if (len > 0) {
                RETURN_IF_INVALID_STATUS(file_.skip(static_cast<uint64_t>(len)))
            }
        }
    }
    return Status::OK();
}

Status SnapshotFileReader::for_each_request(const RequestVisitor& fn) const {
    if (!file_.is_open()) {
        return Status::ERROR("snapshot reader is not open");
    }
    FileReader reader(file_);
    for (int i = 0; i < request_count_; i++) {
        RequestRecord record;
        RETURN_IF_INVALID_STATUS(binary::decode(reader, record))
        if (!is_kv_write_op(record.op)) {
            return Status::ERROR("invalid snapshot request op");
        }
        RETURN_IF_INVALID_STATUS(fn(record))
    }
    return Status::OK();
}

Status SnapshotFileReader::read_chunk(uint64 offset, size_t max_bytes, std::string& data, bool& eof) const {
    if (!file_.is_open()) {
        return Status::ERROR("snapshot reader is not open");
    }
    data.clear();
    eof = false;
    uint64_t file_size{0};
    RETURN_IF_INVALID_STATUS(file_.size(file_size))
    if (offset > file_size) {
        return Status::ERROR("snapshot chunk offset out of range");
    }

    size_t read_len = std::min(max_bytes, static_cast<size_t>(file_size - offset));
    data.resize(read_len);
    if (read_len > 0) {
        RETURN_IF_INVALID_STATUS(file_.pread(offset, data.data(), read_len))
    }
    eof = offset + read_len >= file_size;
    return Status::OK();
}

Status SnapshotStore::open_reader(std::unique_ptr<SnapshotFileReader>& reader, bool& exists) const {
    reader.reset();
    exists = false;
    File file;
    {
        std::lock_guard lock(mutex_);
        RETURN_IF_INVALID_STATUS(File::open_for_read(snapshot_path_, file, exists))
    }
    if (!exists) {
        return Status::OK();
    }

    auto candidate = std::unique_ptr<SnapshotFileReader>(new SnapshotFileReader(std::move(file)));
    RETURN_IF_INVALID_STATUS(candidate->initialize())
    reader = std::move(candidate);
    return Status::OK();
}

Status SnapshotStore::verify_header(const SnapshotFileReader& reader, LogIndex expected_index, Term expected_term) const {
    const SnapshotHeader& actual = reader.header();
    if (actual.apply_index != expected_index || actual.apply_term != expected_term) {
        LOG_WARN(
                "[SnapshotStore] snapshot header mismatch, expected:{}:{}, actual:{}:{}",
                expected_index, expected_term, actual.apply_index, actual.apply_term);
        return Status::CONDITION_NOT_MET(
                fmt::format("snapshot header mismatch, expected index:{} term:{}, actual index:{} term:{}",
                            expected_index, expected_term, actual.apply_index, actual.apply_term));
    }
    return Status::OK();
}

Status SnapshotStore::load_header(SnapshotHeaderPtr& snapshot) const {
    std::unique_ptr<SnapshotFileReader> reader;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(open_reader(reader, exists))
    if (!exists) {
        snapshot.reset();
        return Status::OK();
    }

    if (!snapshot) {
        snapshot = std::make_shared<SnapshotHeader>();
    }
    *snapshot = reader->header();
    return Status::OK();
}

Status SnapshotStore::append_snapshot_chunk(const InstallSnapshotParam& param) {
    std::lock_guard lock(mutex_);
    const std::string session_path = adviskv::receiving_snapshot_file(dir_path_, param.snapshot_index).string();

    File session_file;
    if (param.offset == 0) {
        RETURN_IF_INVALID_STATUS(File::open_for_write(session_path, session_file))
    } else {
        RETURN_IF_INVALID_STATUS(File::open_for_append(session_path, session_file))
    }
    uint64_t current_size{0};
    RETURN_IF_INVALID_STATUS(session_file.size(current_size))
    if (current_size != param.offset) {
        return Status::ERROR("snapshot chunk offset mismatch");
    }
    FileWriter writer(session_file);
    RETURN_IF_INVALID_STATUS(writer.write_raw(param.data.data(), param.data.size()))
    return writer.sync();
}

Status SnapshotStore::finish_snapshot_receive(const InstallSnapshotParam& param) {
    std::lock_guard lock(mutex_);
    const std::string session_path = adviskv::receiving_snapshot_file(dir_path_, param.snapshot_index).string();
    {
        File verify_file;
        bool exists = false;
        RETURN_IF_INVALID_STATUS(File::open_for_read(session_path, verify_file, exists))
        if (!exists) {
            return Status::ERROR("failed to open received snapshot for verify: session file missing");
        }
        SnapshotFileReader reader(std::move(verify_file));
        RETURN_IF_INVALID_STATUS(reader.initialize())
    }
    RETURN_IF_INVALID_STATUS(publish_tmp_snapshot(session_path, snapshot_path_, dir_path_))
    testhook::crash_point("persist.after_remote_snapshot_publish");
    return Status::OK();
}

Status SnapshotStore::publish_tmp_snapshot(const std::string& tmp_path, const std::string& snapshot_path,
                                           const std::string& dir_path) {
    RETURN_IF_INVALID_STATUS(File::rename(tmp_path, snapshot_path))
    RETURN_IF_INVALID_STATUS(File::fsync_dir(dir_path))
    return Status::OK();
}

Status SnapshotStore::write_local_snapshot_candidate(const SnapshotData& snapshot,
                                                     const std::vector<RaftMember>& members, LogIndex capture_index) {
    ADVISKV_METRICS_COUNTER("storage_persist_write_snapshot_request");
    ADVISKV_METRICS_TIMER("storage_persist_write_snapshot");

    const std::string candidate_path = adviskv::candidate_snapshot_file(dir_path_, capture_index).string();

    File candidate_file;
    RETURN_IF_INVALID_STATUS(File::open_for_write(candidate_path, candidate_file))
    RETURN_IF_INVALID_STATUS(write_snapshot_to_file(candidate_file, snapshot, members))
    RETURN_IF_INVALID_STATUS(candidate_file.close())
    return Status::OK();
}

Status SnapshotStore::publish_local_snapshot(LogIndex capture_index, bool& published) {
    published = false;
    std::lock_guard lock(mutex_);

    LogIndex current_index{0};
    {
        File current_file;
        bool exists = false;
        RETURN_IF_INVALID_STATUS(File::open_for_read(snapshot_path_, current_file, exists))
        if (exists) {
            SnapshotFileReader reader(std::move(current_file));
            Status status = reader.initialize();
            if (status.fail()) {
                LOG_WARN("[SnapshotStore] current snapshot invalid, treat index as 0, status:{}",
                         status.to_string());
            } else {
                current_index = reader.header().apply_index;
            }
        }
    }

    const std::string candidate_path = adviskv::candidate_snapshot_file(dir_path_, capture_index).string();

    auto remove_candidate = Defer([&candidate_path]() {
        std::error_code ec;
        std::filesystem::remove(candidate_path, ec);
    });

    if (capture_index <= current_index) {
        LOG_INFO("[SnapshotStore] skip stale local snapshot publish, candidate_index:{}, current_index:{}",
                 capture_index, current_index);
        return Status::OK();
    }

    RETURN_IF_INVALID_STATUS(publish_tmp_snapshot(candidate_path, snapshot_path_, dir_path_))
    published = true;
    testhook::crash_point("persist.after_local_snapshot_publish");
    LOG_INFO("[SnapshotStore] local snapshot published, index:{}, previous_index:{}", capture_index, current_index);
    return Status::OK();
}

}  // namespace adviskv::storage
