#pragma once

#include <memory>
#include <string>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "meta.grpc.pb.h"

namespace adviskv::cli {

struct MetaCliTarget {
    Endpoint endpoint;
    int32_t timeout_ms{3000};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!endpoint.ip.empty(),
                                    "endpoint.ip should not empty")
        RETURN_IF_INVALID_CONDITION(endpoint.port > 0,
                                    "endpoint.port should > 0")
        RETURN_IF_INVALID_CONDITION(timeout_ms > 0, "timeout_ms should > 0")
        return Status::OK();
    }
};

struct TableInfo {
    DatabaseID db_id{-1};
    TableID table_id{-1};
    int32_t shard_count{0};
    int32_t replica_count{0};
    int32 table_state{0};
    std::string last_error_msg;
};

class DirectMetaClient {
   public:
    explicit DirectMetaClient(const MetaCliTarget& target);

    Status create_db(const std::string& db_name, const std::string& zone,
                     DatabaseID* db_id) const;
    Status create_table(const std::string& db_name, const std::string& table_name,
                        int32_t shard_count, int32_t replica_count,
                        TableID* table_id, std::string resource_pool) const;
    Status get_table(const std::string& db_name, const std::string& table_name,
                     TableInfo* table_info) const;

   private:
    MetaCliTarget target_;
    std::unique_ptr<rpc::MetaService::Stub> stub_;
};

}  // namespace adviskv::cli
