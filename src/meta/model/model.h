#pragma once

#include <cstdint>
#include <string>

#include "common/define.h"
#include "common/model/type.h"

namespace adviskv::meta {

enum class TableState : int32_t { ADDING = 1, NORMAL = 2, FAILED = 3, DROPPING = 4, DELETED = 5, ALTERING = 6 };

struct DBMeta {
    DatabaseID db_id;
    std::string db_name;
    std::string zone;

    bool operator==(const DBMeta& other) const {
        if (db_id != other.db_id) return false;
        if (db_name != other.db_name) return false;
        if (zone != other.zone) return false;
        return true;
    }

    DEFINE_OPERATOR_NOT_EQUAL(DBMeta)
};

struct TableMeta {
    TableID table_id;
    int32_t shard_count;
    int32_t replica_count;
    DatabaseID db_id;
    std::string db_name;
    std::string table_name;
    std::string resource_pool;
    TableState state{TableState::ADDING};
    std::string operation_id;
    std::string last_error_msg;
    int64_t create_ts{0};
    int64_t update_ts{0};
    EngineType engine_type{EngineType::MAP};

    bool operator==(const TableMeta& other) const {
        if (table_id != other.table_id) return false;
        if (shard_count != other.shard_count) return false;
        if (replica_count != other.replica_count) return false;
        if (db_id != other.db_id) return false;
        if (db_name != other.db_name) return false;
        if (table_name != other.table_name) return false;
        if (resource_pool != other.resource_pool) return false;
        if (state != other.state) return false;
        if (operation_id != other.operation_id) return false;
        if (last_error_msg != other.last_error_msg) return false;
        if (create_ts != other.create_ts) return false;
        if (update_ts != other.update_ts) return false;
        if (engine_type != other.engine_type) return false;
        return true;
    }

    DEFINE_OPERATOR_NOT_EQUAL(TableMeta)
};

}  // namespace adviskv::meta