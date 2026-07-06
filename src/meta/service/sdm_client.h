#pragma once

#include <grpcpp/impl/channel_interface.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/model/type.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "sdm.grpc.pb.h"

namespace adviskv::meta {

struct SdmTableStatus {
    int32_t table_id{-1};
    SdmTableDesired desired{SdmTableDesired::PRESENT};
    SdmTablePhase phase{SdmTablePhase::CREATING};
    std::string last_error_msg;
    std::string operation_id;
};

class ISdmClient {
   public:
    virtual ~ISdmClient() = default;

    virtual Status call_place_table(const TableMeta& table_meta) = 0;
    virtual Status call_drop_table(const TableMeta& table_meta) = 0;
    virtual Status call_alter_table_replica_count(
        const TableMeta& table_meta) = 0;
    virtual Status get_table_status(const TableMeta& table_meta,
                                    SdmTableStatus* table_status) = 0;
};

class SdmClient : public ISdmClient {
   public:
    explicit SdmClient(const std::shared_ptr<grpc::ChannelInterface>& channel,
                       int32_t timeout_ms = 3000)
        : stub_(sdm_rpc::SdmService::NewStub(channel)),
          timeout_ms_(timeout_ms > 0 ? timeout_ms : 3000) {}

    Status call_place_table(const TableMeta& table_meta) override;
    Status call_drop_table(const TableMeta& table_meta) override;
    Status call_alter_table_replica_count(
        const TableMeta& table_meta) override;
    Status get_table_status(const TableMeta& table_meta,
                            SdmTableStatus* table_status) override;

   private:
    using SdmClientStub = std::unique_ptr<sdm_rpc::SdmService::Stub>;

    SdmClientStub stub_;
    int32_t timeout_ms_{3000};
};

}  // namespace adviskv::meta