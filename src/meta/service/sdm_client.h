#pragma once

#include <grpcpp/impl/channel_interface.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "meta/catalog/meta_types.h"
#include "sdm.grpc.pb.h"

namespace adviskv::meta {

struct SdmTableStatus {
    int32_t table_id{-1};
    int32_t desired{0};
    int32_t phase{0};
    std::string last_error_msg;
    std::string operation_id;
};

class ISdmClient {
   public:
    virtual ~ISdmClient() = default;

    virtual Status call_place_table(const TableMeta& table_meta) = 0;
    virtual Status call_drop_table(const TableMeta& table_meta) = 0;
    virtual Status call_place_db(const DBMeta& db_meta) = 0;
    virtual Status get_table_status(const TableMeta& table_meta,
                                    SdmTableStatus* table_status) = 0;
};

class SdmClient : public ISdmClient {
   public:
    explicit SdmClient(const std::shared_ptr<grpc::ChannelInterface>& channel)
        : stub_(rpc::ShardingManagerService::NewStub(channel)) {}

    Status call_place_table(const TableMeta& table_meta) override;
    Status call_drop_table(const TableMeta& table_meta) override;
    Status call_place_db(const DBMeta& db_meta) override;
    Status get_table_status(const TableMeta& table_meta,
                            SdmTableStatus* table_status) override;

   private:
    using SdmClientStub = std::unique_ptr<rpc::ShardingManagerService::Stub>;

    SdmClientStub stub_;
};

}  // namespace adviskv::meta