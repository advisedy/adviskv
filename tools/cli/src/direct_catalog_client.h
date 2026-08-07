#pragma once

#include <memory>
#include <string>

#include "common/define.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "common/model/type.h"
#include "catalog.grpc.pb.h"
#include "catalog/model/model.h"

namespace adviskv::cli {

struct CatalogCliTarget {
    Endpoint endpoint;
    int32_t timeout_ms{3000};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!endpoint.ip.empty(), "endpoint.ip should not empty")
        RETURN_IF_INVALID_CONDITION(endpoint.port > 0, "endpoint.port should > 0")
        RETURN_IF_INVALID_CONDITION(timeout_ms > 0, "timeout_ms should > 0")
        return Status::OK();
    }
};

struct TableInfo {
    DatabaseID db_id{-1};
    TableID table_id{-1};
    int32_t shard_count{0};
    int32_t replica_count{0};
    EngineType engine_type{EngineType::MAP};
    catalog::TableState table_state{catalog::TableState::ADDING};
    std::string last_error_msg;
};

class DirectCatalogClient {
public:
    explicit DirectCatalogClient(const CatalogCliTarget& target);

    Status create_db(const std::string& db_name, const std::string& zone, DatabaseID* db_id) const;
    Status drop_db(const std::string& db_name, DatabaseID* db_id) const;
    Status create_table(const std::string& db_name, const std::string& table_name, int32_t shard_count,
                        int32_t replica_count, TableID* table_id, std::string resource_pool,
                        EngineType engine_type = EngineType::MAP) const;
    Status alter_table_replica_count(const std::string& db_name, const std::string& table_name, int32_t replica_count,
                                     TableID* table_id) const;
    Status get_table(const std::string& db_name, const std::string& table_name, TableInfo* table_info) const;

private:
    CatalogCliTarget target_;
    std::unique_ptr<catalog_rpc::CatalogService::Stub> stub_;
};

}  // namespace adviskv::cli
