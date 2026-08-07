#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <grpcpp/impl/channel_interface.h>

#include "common/model/type.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "catalog/model/model.h"
#include "topo.grpc.pb.h"

namespace adviskv::catalog {

struct TopoTableStatus {
    int32_t table_id{-1};
    TopoTableDesired desired{TopoTableDesired::PRESENT};
    TopoTablePhase phase{TopoTablePhase::CREATING};
    std::string last_error_msg;
    std::string operation_id;
};

class ITopoClient {
public:
    virtual ~ITopoClient() = default;

    virtual Status call_place_table(const TableDesc& table_desc) = 0;
    virtual Status call_drop_table(const TableDesc& table_desc) = 0;
    virtual Status call_alter_table_replica_count(const TableDesc& table_desc) = 0;
    virtual Status get_table_status(const TableDesc& table_desc, TopoTableStatus* table_status) = 0;
};

class TopoClient : public ITopoClient {
public:
    explicit TopoClient(const std::shared_ptr<grpc::ChannelInterface>& channel, int32_t timeout_ms = 3000)
            : stub_(topo_rpc::TopoService::NewStub(channel)), timeout_ms_(timeout_ms > 0 ? timeout_ms : 3000) {}

    Status call_place_table(const TableDesc& table_desc) override;
    Status call_drop_table(const TableDesc& table_desc) override;
    Status call_alter_table_replica_count(const TableDesc& table_desc) override;
    Status get_table_status(const TableDesc& table_desc, TopoTableStatus* table_status) override;

private:
    using TopoClientStub = std::unique_ptr<topo_rpc::TopoService::Stub>;

    TopoClientStub stub_;
    int32_t timeout_ms_{3000};
};

}  // namespace adviskv::catalog