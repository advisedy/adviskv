
#pragma once

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include <cstdint>
#include <string>
namespace adviskv::sdm {

// Placement Param
struct PlaceTableParam {
  int32_t db_id;
  int32_t table_id;
  std::string db_name;
  std::string table_name;
  int32_t replica_count;
  int32_t shard_count;
  std::string resource_pool;
  Status validate() const {
    RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
    RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                "table_name should not empty")
    RETURN_IF_INVALID_CONDITION(replica_count > 0,
                                "replica_count should be greater than 0")
    RETURN_IF_INVALID_CONDITION(shard_count >= 0,
                                "shard_count should be greater than 0")
    return Status::OK();
  }
};

struct PlaceDBParam {
  int32_t db_id{-1};
  std::string db_name;
  std::string zone;
  Status validate() const {
    RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
    RETURN_IF_INVALID_CONDITION(db_id != -1, "db_id should not is -1")
    return Status::OK();
  }
};

//node service

struct RegisterNodeParam{
    NodeID node_id;
    std::string ip;
    int32_t port;
    std::string resource_pool;
    std::string dc;
    Status validate()const{
        RETURN_IF_INVALID_CONDITION(!node_id.empty(), "node_id should not empty")
        RETURN_IF_INVALID_CONDITION(!ip.empty(), "ip should not empty")
        RETURN_IF_INVALID_CONDITION(port > 0 , "port should > 0")
        return Status::OK();
    }
};


//route service

struct GetRouteParam{
    std::string db_name;
    std::string table_name;
    Key key;

    Status validate()const{
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        return Status::OK();
    }
};



} // namespace adviskv