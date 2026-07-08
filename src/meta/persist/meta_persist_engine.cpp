#include "meta/persist/meta_persist_engine.h"

#include <cerrno>
#include <cstring>
#include <filesystem>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/defer.h"
#include "common/define.h"
#include "common/framed_record_codec.h"
#include "common/log.h"

namespace adviskv::meta {
namespace {

static constexpr int64 K_MAX_META_PAYLOAD_BYTES = 64 * 1024 * 1024;

class MetaRecordCodec {
public:
    using ObjectType = PersistedMetaRecord;
    using LenType = int64;

    LenType max_payload_len() const { return K_MAX_META_PAYLOAD_BYTES; }

    void encode_payload(const ObjectType& record, EncodeBuffer& buf) const {
        buf.write(record.next_db_id);
        buf.write(record.next_table_id);

        buf.write<int32>(static_cast<int32>(record.db_meta_map.size()));
        for (const auto& [db_id, db_meta] : record.db_meta_map) {
            UNUSED(db_id);
            buf.write(db_meta.db_id);
            buf.write(db_meta.db_name);
            buf.write(db_meta.zone);
        }

        buf.write<int32>(static_cast<int32>(record.table_id2table_meta.size()));
        for (const auto& [table_id, table_meta] : record.table_id2table_meta) {
            UNUSED(table_id);
            buf.write(table_meta.table_id);
            buf.write(table_meta.shard_count);
            buf.write(table_meta.replica_count);
            buf.write(table_meta.db_id);
            buf.write(table_meta.db_name);
            buf.write(table_meta.table_name);
            buf.write(table_meta.resource_pool);
            buf.write(static_cast<int32>(table_meta.state));
            buf.write(table_meta.operation_id);
            buf.write(table_meta.last_error_msg);
            buf.write(table_meta.create_ts);
            buf.write(table_meta.update_ts);
            buf.write(static_cast<int32>(table_meta.engine_type));
        }
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& record) const {
        record = {};
        RETURN_IF_INVALID_READ(buf, record.next_db_id)
        RETURN_IF_INVALID_READ(buf, record.next_table_id)

        int32 db_count{0};
        RETURN_IF_INVALID_READ(buf, db_count)
        if (db_count < 0) {
            return Status{StatusCode::ERROR, "invalid db_count"};
        }
        for (int32 i = 0; i < db_count; ++i) {
            DBMeta db_meta;
            RETURN_IF_INVALID_READ(buf, db_meta.db_id)
            RETURN_IF_INVALID_READ(buf, db_meta.db_name)
            RETURN_IF_INVALID_READ(buf, db_meta.zone)
            record.db_meta_map[db_meta.db_id] = std::move(db_meta);
        }

        int32 table_count{0};
        RETURN_IF_INVALID_READ(buf, table_count)
        if (table_count < 0) {
            return Status{StatusCode::ERROR, "invalid table_count"};
        }
        for (int32 i = 0; i < table_count; ++i) {
            TableMeta table_meta;
            RETURN_IF_INVALID_READ(buf, table_meta.table_id)
            RETURN_IF_INVALID_READ(buf, table_meta.shard_count)
            RETURN_IF_INVALID_READ(buf, table_meta.replica_count)
            RETURN_IF_INVALID_READ(buf, table_meta.db_id)
            RETURN_IF_INVALID_READ(buf, table_meta.db_name)
            RETURN_IF_INVALID_READ(buf, table_meta.table_name)
            RETURN_IF_INVALID_READ(buf, table_meta.resource_pool)
            int32 state{0};
            RETURN_IF_INVALID_READ(buf, state)
            table_meta.state = static_cast<TableState>(state);
            RETURN_IF_INVALID_READ(buf, table_meta.operation_id)
            RETURN_IF_INVALID_READ(buf, table_meta.last_error_msg)
            RETURN_IF_INVALID_READ(buf, table_meta.create_ts)
            RETURN_IF_INVALID_READ(buf, table_meta.update_ts)
            int32 engine_type{0};
            RETURN_IF_INVALID_READ(buf, engine_type)
            table_meta.engine_type = static_cast<EngineType>(engine_type);
            switch (table_meta.engine_type) {
                case EngineType::MAP:
                case EngineType::ROCKSDB:
                    break;
                default:
                    return Status::ERROR("invalid table_meta engine_type");
            }
            record.table_id2table_meta[table_meta.table_id] = std::move(table_meta);
        }
        return Status::OK();
    }
};

}  // namespace

MetaPersistEngine::MetaPersistEngine(const std::string& data_dir) : data_dir_(data_dir) {}

MetaPersistEngine::~MetaPersistEngine() { close(); }

Status MetaPersistEngine::init() {
    meta_data_path_ = data_dir_ + "/meta_data";
    meta_data_tmp_path_ = data_dir_ + "/meta_data.tmp";

    std::error_code ec;
    std::filesystem::create_directories(data_dir_, ec);
    if (ec) {
        return Status::ERROR(fmt::format("failed to create meta data dir: {}, error: {}", data_dir_, ec.message()));
    }
    LOG_DEBUG("meta persist engine init, data_dir_={}, meta_data_path_={}", data_dir_, meta_data_path_);
    return Status::OK();
}

Status MetaPersistEngine::close() { return Status::OK(); }

Status MetaPersistEngine::save_meta(const PersistedMetaRecord& record) {
    int fd = ::open(meta_data_tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status{StatusCode::ERROR, fmt::format("failed to open meta tmp file: {}", meta_data_tmp_path_)};
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    RETURN_IF_INVALID_STATUS(FramedRecord<MetaRecordCodec>::encode_to_fd(fd, record))

    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync meta tmp file");
    }
    if (::close(fd) != 0) {
        fd = -1;
        return Status::ERROR("failed to close meta tmp file");
    }
    fd = -1;

    if (::rename(meta_data_tmp_path_.c_str(), meta_data_path_.c_str()) != 0) {
        return Status{StatusCode::ERROR, fmt::format("failed to rename meta data file: {}", meta_data_path_)};
    }

    RETURN_IF_INVALID_STATUS(func::fsync_dir(data_dir_))
    LOG_DEBUG(
            "meta persist engine save_meta success, db_count={}, "
            "table_count={}, next_db_id={}, next_table_id={}",
            record.db_meta_map.size(), record.table_id2table_meta.size(), record.next_db_id, record.next_table_id);
    return Status::OK();
}

Status MetaPersistEngine::load_meta(PersistedMetaRecord& record) {
    record = {};

    int fd = ::open(meta_data_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            LOG_DEBUG("meta data file not found, starting with empty meta");
            return Status::OK();
        }
        return Status{StatusCode::ERROR, fmt::format("failed to open meta data file: {}", meta_data_path_)};
    }
    auto fd_guard = Defer([fd]() { ::close(fd); });

    Status status = FramedRecord<MetaRecordCodec>::decode_from_fd(fd, record);
    if (status.code() == StatusCode::GET_EOF) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(status)

    LOG_DEBUG(
            "meta persist engine load_meta success, db_count={}, "
            "table_count={}, next_db_id={}, next_table_id={}",
            record.db_meta_map.size(), record.table_id2table_meta.size(), record.next_db_id, record.next_table_id);
    return Status::OK();
}

}  // namespace adviskv::meta
