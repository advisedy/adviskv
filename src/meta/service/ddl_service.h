#pragma once


#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include <cstdint>
#include <grpcpp/impl/channel_interface.h>
#include <memory>
#include <string>
#include "common/define.h"
#include "sdm.grpc.pb.h"

namespace adviskv{

struct SdmClient;



struct CreateDBParam {
    const std::string& db_name;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        return Status::OK();
    }
};

struct CreateTableParam {
    const std::string& db_name;
    const std::string& table_name;
    int32_t shard_count;
    int32_t replica_count;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(shard_count > 0, "shard count should > 0")
        RETURN_IF_INVALID_CONDITION(replica_count >= 0, "replica count should >= 0")
        return Status::OK();
    }

};

struct GetTableParam{
    //可选
   const std::string& db_name;
   const std::string& table_name;
   //可选
   int32_t table_id{-1};
   Status validate() const {
    RETURN_IF_INVALID_CONDITION(table_id != -1 or (!db_name.empty() and !table_name.empty()), "please fill table_id or (db_name, table_name)")
    return Status::OK();
}

};

// struct SdmPlaceTableParam{
//     int32_t db_id;
//     int32_t table_id;
//     int32_t shard_count;
//     int32_t replica_count;
// };

using SdmClientStub = std::unique_ptr<rpc::ShardingManagerService::Stub>;

class SdmClient {
public:
    explicit SdmClient(const std::shared_ptr<grpc::ChannelInterface>& channel)
        : stub_(rpc::ShardingManagerService::NewStub(channel)) {}

    std::unique_ptr<rpc::ShardingManagerService::Stub>& client() {
        return stub_;
    }

    Status call_place_table(const TableMeta& table_meta);

private:
    SdmClientStub stub_;
};

class DdlService{

public:

    explicit DdlService(CatalogManager* catalog_manager, SdmClient* sdm_client);

    //负责一些会涉及到catalog和sdm的操作
    Status create_table(const CreateTableParam& param, TableMeta* table_meta);
    Status create_db(const CreateDBParam& param, DBMeta* db_meta);
    Status get_table(const GetTableParam& param, TableMeta* table_meta);

private:


    SdmClient* sdm_client_;
    CatalogManager* catalog_manager_;
};

}