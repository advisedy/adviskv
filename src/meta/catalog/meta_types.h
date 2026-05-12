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
};

struct TableMeta {
    TableID table_id;
    int32_t shard_count;
    int32_t replica_count;
    DatabaseID db_id;
    std::string db_name;
    std::string table_name;
    std::string resource_pool;
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
