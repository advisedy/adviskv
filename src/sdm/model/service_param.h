
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"
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
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                    "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(replica_count > 0,
                                    "replica_count should be greater than 0")
        RETURN_IF_INVALID_CONDITION(shard_count >= 0,
                                    "shard_count should be greater than 0")
        return Status::OK();
    }
};

struct PlaceNodesParam {
    std::string resource_pool;
    int32_t shard_count{0};
    int32_t replica_count{0};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!resource_pool.empty(),
                                    "resource_pool should not empty")
        RETURN_IF_INVALID_CONDITION(shard_count > 0,
                                    "shard_count should be greater than 0")
        RETURN_IF_INVALID_CONDITION(replica_count > 0,
                                    "replica_count should be greater than 0")
        return Status::OK();
    }
};

struct ShardPlacement {
    ShardIndex shard_index{0};
    std::vector<NodePtr> nodes;
};

struct TablePlacementResult {
    std::vector<ShardPlacement> shards;
};

struct PlaceDBParam {
    int32_t db_id{-1};
    std::string db_name;
    std::string zone;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(db_id != -1, "db_id should not is -1")
        return Status::OK();
    }
};

// storage client
struct CreateReplicaParam {
    ReplicaID replica_id;
    EngineType engine_type{EngineType::MAP};
    std::vector<PeerMember> members;
    Endpoint endpoint; // 对端的endpoint

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(replica_id.table_id >= 0,
                                    "table_id should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.shard_index >= 0,
                                    "shard_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.replica_index >= 0,
                                    "replica_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(!members.empty(),
                                    "members should not empty")
        RETURN_IF_INVALID_CONDITION(!endpoint.ip.empty(),
                                    "endpoint ip should not empty")
        RETURN_IF_INVALID_CONDITION(endpoint.port > 0,
                                    "endpoint port should greater than 0")
        return Status::OK();
    }
};

// node service

struct RegisterNodeParam {
    NodeID node_id;
    std::string ip;
    int32_t port;
    std::string resource_pool;
    std::string dc;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!node_id.empty(),
                                    "node_id should not empty")
        RETURN_IF_INVALID_CONDITION(!ip.empty(), "ip should not empty")
        RETURN_IF_INVALID_CONDITION(port > 0, "port should > 0")
        return Status::OK();
    }
};

// route service

struct GetRouteParam {
    std::string db_name;
    std::string table_name;
    Key key;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                    "table_name should not empty")
        return Status::OK();
    }
};

// heartbeat service

/*
message HeartBeatRequest{
    string node_id = 1;
    string ip = 2;
    int32 port = 3;
    string resource_pool = 4;
    string dc = 5;
    adviskv.pb.NodeStatus node_status = 6;
    repeated HeartBeatReplicaInfo replica_info_list = 7;
}

message HeartBeatReplicaSepc{
    int32 table_id = 1;
    int32 shard_id = 2;
    int32 replica_index = 3;
    adviskv.pb.ReplicaRole role = 4;    
}

message HeartBeatResponse{
    adviskv.pb.BaseRsp base_rsp = 1;
    repeated HeartBeatReplicaSepc replica_spec_list = 2;
}
*/

struct HeartBeatReplicaInfo{
  ShardID shard_id;
  ReplicaIndex replica_index;
  ReplicaRole role;
  ReplicaStatus status;
};

struct HeartBeatParam {
    NodeID node_id;
    std::string ip;
    int32_t port{-1};
    std::string resoure_pool_name{"default"};
    std::string dc;
    std::vector<HeartBeatReplicaInfo> replica_list;
    int64_t last_heartbeat_ts{0};

    Status validate()const{
      RETURN_IF_INVALID_CONDITION(!node_id.empty(), "node need id");
      RETURN_IF_INVALID_CONDITION(!ip.empty(), "ip should not empty");
      RETURN_IF_INVALID_CONDITION(port!=-1, "port should not empty");
      RETURN_IF_INVALID_CONDITION(!resoure_pool_name.empty(), "resource_pool should not empty");
      RETURN_IF_INVALID_CONDITION(!dc.empty(), "dc should not empty");
      //TODO check replica info 
      return Status::OK();
    }
};

// struct HeartBeatResultEntry{
//     ReplicaID replica_id;
//     ReplicaRole replica_role;
// };

// struct HeartBeatResult{
//   std::vector<HeartBeatResultEntry> entry_list;
// };


}  // namespace adviskv::sdm
