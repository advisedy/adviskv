
#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"
#include "meta/model/model.h"
namespace adviskv::meta {

struct PlaceNodesParam {
    std::string resource_pool;
    int32_t shard_count{0};
    int32_t replica_count{0};
    std::unordered_set<NodeID> excluded_node_ids;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!resource_pool.empty(), "resource_pool should not empty")
        RETURN_IF_INVALID_CONDITION(shard_count > 0, "shard_count should be greater than 0")
        RETURN_IF_INVALID_CONDITION(replica_count > 0, "replica_count should be greater than 0")
        return Status::OK();
    }
};

struct ShardPlacement {
    ShardIndex shard_index{0};
    std::vector<Node> nodes;
};

struct TablePlacementResult {
    std::vector<ShardPlacement> shards;
};

// Storage客户端参数
struct CreateReplicaParam {
    ReplicaID replica_id;
    EngineType engine_type{EngineType::MAP};
    std::vector<PeerMember> members;
    Endpoint endpoint;  // 对端的endpoint

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(replica_id.table_id >= 0, "table_id should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.shard_index >= 0, "shard_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.replica_seq >= 0, "replica_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(!members.empty(), "members should not empty")
        RETURN_IF_INVALID_CONDITION(!endpoint.ip.empty(), "endpoint ip should not empty")
        RETURN_IF_INVALID_CONDITION(endpoint.port > 0, "endpoint port should greater than 0")
        return Status::OK();
    }
};

struct DeleteReplicaParam {
    ReplicaID replica_id;
    Endpoint endpoint;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(replica_id.table_id >= 0, "table_id should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.shard_index >= 0, "shard_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.replica_seq >= 0, "replica_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(!endpoint.ip.empty(), "endpoint ip should not empty")
        RETURN_IF_INVALID_CONDITION(endpoint.port > 0, "endpoint port should greater than 0")
        return Status::OK();
    }
};

struct GetReplicaInfoParam {
    ReplicaID replica_id;
    Endpoint endpoint;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(replica_id.table_id >= 0, "table_id should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.shard_index >= 0, "shard_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.replica_seq >= 0, "replica_index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(!endpoint.ip.empty(), "endpoint ip should not empty")
        RETURN_IF_INVALID_CONDITION(endpoint.port > 0, "endpoint port should greater than 0")
        return Status::OK();
    }
};

// 节点服务参数

struct RegisterNodeParam {
    NodeID node_id;
    std::string ip;
    int32_t port;
    std::string resource_pool;
    std::string dc;
    int64_t last_heartbeat_ts{0};
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!node_id.empty(), "node_id should not empty")
        RETURN_IF_INVALID_CONDITION(!ip.empty(), "ip should not empty")
        RETURN_IF_INVALID_CONDITION(port > 0, "port should > 0")
        RETURN_IF_INVALID_CONDITION(!resource_pool.empty(), "resource_pool should not empty")
        RETURN_IF_INVALID_CONDITION(!dc.empty(), "dc should not empty")
        return Status::OK();
    }
};

// 路由服务参数

struct GetTableRoutesParam {
    std::string db_name;
    std::string table_name;
    bool include_unavailable{false};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        return Status::OK();
    }
};

struct HeartBeatReplicaInfo {
    ReplicaID replica_id;
    ReplicaRole role;
    StorageReplicaStatus storage_status;
    Term term;  // 用来帮助判断leader，term高的优先被认定是leader
    RaftMemberType member_type{RaftMemberType::NON_MEMBER};
    std::vector<RaftMember> full_membership;
};

struct HeartBeatParam {
    NodeID node_id;
    std::string ip;
    int32_t port{-1};
    std::string resoure_pool_name{"default"};
    std::string dc;
    std::vector<HeartBeatReplicaInfo> replica_list;
    int64_t last_heartbeat_ts{0};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!node_id.empty(), "node need id");
        RETURN_IF_INVALID_CONDITION(!ip.empty(), "ip should not empty");
        RETURN_IF_INVALID_CONDITION(port > 0, "port should not empty");
        RETURN_IF_INVALID_CONDITION(!resoure_pool_name.empty(), "resource_pool should not empty");
        RETURN_IF_INVALID_CONDITION(!dc.empty(), "dc should not empty");
        for (const HeartBeatReplicaInfo& info : replica_list) {
            RETURN_IF_INVALID_CONDITION(info.replica_id.replica_seq >= 0, "info replica seq should >= 0");
            RETURN_IF_INVALID_CONDITION(info.replica_id.table_id >= 0, "info table id should >= 0");
            RETURN_IF_INVALID_CONDITION(info.replica_id.shard_index >= 0, "info shard index should >= 0");
            RETURN_IF_INVALID_CONDITION(info.term >= 0, "info term should >= 0");
        }
        return Status::OK();
    }
};

struct HeartBeatResult {
    std::vector<ExpectedReplica> expects;
};

struct CreateDBParam {
    std::string db_name;
    std::string zone;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        return Status::OK();
    }
};

struct DropDBParam {
    std::string db_name;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        return Status::OK();
    }
};

struct CreateTableParam {
    std::string db_name;
    std::string table_name;
    int32_t shard_count{0};
    int32_t replica_count{0};
    std::string resource_pool;
    EngineType engine_type{EngineType::MAP};
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(shard_count > 0 && shard_count <= static_cast<int32_t>(K_HASH_BUCKET_COUNT),
                                    "shard count is not valid")
        RETURN_IF_INVALID_CONDITION(replica_count >= 0, "replica count should >= 0")
        switch (engine_type) {
            case EngineType::MAP:
                break;
            default:
                return Status::INVALID_ARGUMENT("engine_type should be valid");
        }
        return Status::OK();
    }
};

struct GetTableParam {
    std::string db_name;
    std::string table_name;
    bool use_table_id{false};
    TableID table_id{-1};
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(
                (use_table_id && table_id >= 0) || (!use_table_id && !db_name.empty() && !table_name.empty()),
                "please fill (table_id) or (db_name, table_name)")
        return Status::OK();
    }
};

struct DropTableByNameParam {
    std::string db_name;
    std::string table_name;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        return Status::OK();
    }
};

struct AlterTableReplicaCountParam {
    std::string db_name;
    std::string table_name;
    int32_t replica_count{0};
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(replica_count >= 0, "replica_count should >= 0")
        return Status::OK();
    }
};

}  // namespace adviskv::meta
