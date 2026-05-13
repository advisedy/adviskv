#pragma once

#include <cstdint>
#include <string>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::meta {

enum class TableState {
    ADDING = 1,
    NORMAL = 2,
    DROPPING = 3,
    DELETED = 4
};

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
};

struct TableMeta {
    TableID table_id;
    int32_t shard_count;
    int32_t replica_count;
    DatabaseID db_id;
    std::string db_name;
    std::string table_name;
    std::string resource_pool;

    bool operator==(const TableMeta& other) const {
        if (table_id != other.table_id) return false;
        if (shard_count != other.shard_count) return false;
        if (replica_count != other.replica_count) return false;
        if (db_id != other.db_id) return false;
        if (db_name != other.db_name) return false;
        if (table_name != other.table_name) return false;
        if (resource_pool != other.resource_pool) return false;
        return true;
    }
};

struct CreateDBMetaParam {
    std::string db_name;
    std::string zone;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        return Status::OK();
    }
};

struct CreateTableMetaParam {
    std::string db_name;
    std::string table_name;
    int32_t shard_count;
    int32_t replica_count;
    std::string resource_pool;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(shard_count > 0, "shard count should > 0")
        RETURN_IF_INVALID_CONDITION(replica_count >= 0, "replica count should >= 0")
        return Status::OK();
    }
};

}  // namespace adviskv::meta