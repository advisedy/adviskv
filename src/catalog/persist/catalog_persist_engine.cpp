#include "catalog/persist/catalog_persist_engine.h"

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

namespace adviskv::catalog {
namespace {

static constexpr int64 K_MAX_CATALOG_PAYLOAD_BYTES = 64 * 1024 * 1024;

class CatalogRecordCodec {
public:
    using ObjectType = CatalogPersistedRecord;
    using LenType = int64;

    LenType max_payload_len() const { return K_MAX_CATALOG_PAYLOAD_BYTES; }

    void encode_payload(const ObjectType& record, EncodeBuffer& buf) const {
        buf.write(record.next_db_id);
        buf.write(record.next_table_id);

        buf.write<int32>(static_cast<int32>(record.db_desc_map.size()));
        for (const auto& [db_id, db_desc] : record.db_desc_map) {
            UNUSED(db_id);
            buf.write(db_desc.db_id);
            buf.write(db_desc.db_name);
            buf.write(db_desc.zone);
        }

        buf.write<int32>(static_cast<int32>(record.table_id2table_desc.size()));
        for (const auto& [table_id, table_desc] : record.table_id2table_desc) {
            UNUSED(table_id);
            buf.write(table_desc.table_id);
            buf.write(table_desc.shard_count);
            buf.write(table_desc.replica_count);
            buf.write(table_desc.db_id);
            buf.write(table_desc.db_name);
            buf.write(table_desc.table_name);
            buf.write(table_desc.resource_pool);
            buf.write(static_cast<int32>(table_desc.state));
            buf.write(table_desc.operation_id);
            buf.write(table_desc.last_error_msg);
            buf.write(table_desc.create_ts);
            buf.write(table_desc.update_ts);
            buf.write(static_cast<int32>(table_desc.engine_type));
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
            DatabaseDesc db_desc;
            RETURN_IF_INVALID_READ(buf, db_desc.db_id)
            RETURN_IF_INVALID_READ(buf, db_desc.db_name)
            RETURN_IF_INVALID_READ(buf, db_desc.zone)
            record.db_desc_map[db_desc.db_id] = std::move(db_desc);
        }

        int32 table_count{0};
        RETURN_IF_INVALID_READ(buf, table_count)
        if (table_count < 0) {
            return Status{StatusCode::ERROR, "invalid table_count"};
        }
        for (int32 i = 0; i < table_count; ++i) {
            TableDesc table_desc;
            RETURN_IF_INVALID_READ(buf, table_desc.table_id)
            RETURN_IF_INVALID_READ(buf, table_desc.shard_count)
            RETURN_IF_INVALID_READ(buf, table_desc.replica_count)
            RETURN_IF_INVALID_READ(buf, table_desc.db_id)
            RETURN_IF_INVALID_READ(buf, table_desc.db_name)
            RETURN_IF_INVALID_READ(buf, table_desc.table_name)
            RETURN_IF_INVALID_READ(buf, table_desc.resource_pool)
            int32 state{0};
            RETURN_IF_INVALID_READ(buf, state)
            table_desc.state = static_cast<TableState>(state);
            RETURN_IF_INVALID_READ(buf, table_desc.operation_id)
            RETURN_IF_INVALID_READ(buf, table_desc.last_error_msg)
            RETURN_IF_INVALID_READ(buf, table_desc.create_ts)
            RETURN_IF_INVALID_READ(buf, table_desc.update_ts)
            int32 engine_type{0};
            RETURN_IF_INVALID_READ(buf, engine_type)
            table_desc.engine_type = static_cast<EngineType>(engine_type);
            switch (table_desc.engine_type) {
                case EngineType::MAP:
                    break;
                default:
                    return Status::ERROR("invalid table_desc engine_type");
            }
            record.table_id2table_desc[table_desc.table_id] = std::move(table_desc);
        }
        return Status::OK();
    }
};

}  // namespace

CatalogPersistEngine::CatalogPersistEngine(const std::string& data_dir)
        : data_dir_(data_dir),
          catalog_data_path_(data_dir_ + "/catalog_data"),
          catalog_data_tmp_path_(data_dir_ + "/catalog_data.tmp") {}

CatalogPersistEngine::~CatalogPersistEngine() { close(); }

Status CatalogPersistEngine::init() {
    std::error_code ec;
    std::filesystem::create_directories(data_dir_, ec);
    if (ec) {
        return Status::ERROR(fmt::format("failed to create catalog data dir: {}, error: {}", data_dir_, ec.message()));
    }
    LOG_DEBUG("catalog persist engine init, data_dir_={}, catalog_data_path_={}", data_dir_, catalog_data_path_);
    return Status::OK();
}

Status CatalogPersistEngine::close() { return Status::OK(); }

Status CatalogPersistEngine::save_catalog(const CatalogPersistedRecord& record) {
    int fd = ::open(catalog_data_tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status{StatusCode::ERROR, fmt::format("failed to open catalog tmp file: {}", catalog_data_tmp_path_)};
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    RETURN_IF_INVALID_STATUS(FramedRecord<CatalogRecordCodec>::encode_to_fd(fd, record))

    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync catalog tmp file");
    }
    if (::close(fd) != 0) {
        fd = -1;
        return Status::ERROR("failed to close catalog tmp file");
    }
    fd = -1;

    if (::rename(catalog_data_tmp_path_.c_str(), catalog_data_path_.c_str()) != 0) {
        return Status{StatusCode::ERROR, fmt::format("failed to rename catalog data file: {}", catalog_data_path_)};
    }

    RETURN_IF_INVALID_STATUS(func::fsync_dir(data_dir_))
    LOG_DEBUG(
            "catalog persist engine save_catalog success, db_count={}, "
            "table_count={}, next_db_id={}, next_table_id={}",
            record.db_desc_map.size(), record.table_id2table_desc.size(), record.next_db_id, record.next_table_id);
    return Status::OK();
}

Status CatalogPersistEngine::load_catalog(CatalogPersistedRecord& record) {
    record = {};

    int fd = ::open(catalog_data_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            LOG_DEBUG("catalog data file not found, starting with empty catalog");
            return Status::OK();
        }
        return Status{StatusCode::ERROR, fmt::format("failed to open catalog data file: {}", catalog_data_path_)};
    }
    auto fd_guard = Defer([fd]() { ::close(fd); });

    Status status = FramedRecord<CatalogRecordCodec>::decode_from_fd(fd, record);
    if (status.code() == StatusCode::GET_EOF) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(status)

    LOG_DEBUG(
            "catalog persist engine load_catalog success, db_count={}, "
            "table_count={}, next_db_id={}, next_table_id={}",
            record.db_desc_map.size(), record.table_id2table_desc.size(), record.next_db_id, record.next_table_id);
    return Status::OK();
}

}  // namespace adviskv::catalog
