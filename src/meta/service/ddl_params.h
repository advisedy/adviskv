#pragma once

#include <cstdint>
#include <string>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::meta {

struct CreateDBParam {
    std::string db_name;
    std::string zone;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        return Status::OK();
    }
};

struct DropDBParam {
    std::string db_name;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        return Status::OK();
    }
};

struct CreateTableParam {
    std::string db_name;
    std::string table_name;
    int32_t shard_count;
    int32_t replica_count;
    std::string resource_pool;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                    "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(shard_count > 0, "shard count should > 0")
        // replica_count 是可以等于0的
        RETURN_IF_INVALID_CONDITION(replica_count >= 0,
                                    "replica count should >= 0")
        return Status::OK();
    }
};

struct GetTableParam {
    // 可选
    std::string db_name;
    std::string table_name;
    // 可选
    bool use_table_id{false};
    TableID table_id{-1};

    // 上面两个选一个就可以了，如果use_table_id = true就优先走id

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(
            (use_table_id && table_id >= 0) ||
                (!use_table_id && !db_name.empty() && !table_name.empty()),
            "please fill (table_id) or (db_name, table_name)")
        return Status::OK();
    }
};

struct DropTableParam {
    std::string db_name;
    std::string table_name;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                    "table_name should not empty")
        return Status::OK();
    }
};

struct AlterTableReplicaCountParam {
    std::string db_name;
    std::string table_name;
    int32_t replica_count{0};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                    "table_name should not empty")
        // replica_count 是可以等于0的
        RETURN_IF_INVALID_CONDITION(replica_count >= 0,
                                    "replica_count should >= 0")
        return Status::OK();
    }
};

}  // namespace adviskv::meta