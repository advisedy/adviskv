#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "common.pb.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::sdsdk {

struct Endpoint {
    std::string ip;
    int32_t port{0};
};

struct ReplicaKey {
    TableID table_id{-1};
    ShardID shard_id{-1};
    int32_t replica_index{-1};

    bool operator==(const ReplicaKey& other) const {
        return table_id == other.table_id && shard_id == other.shard_id &&
               replica_index == other.replica_index;
    }
};

struct ReplicaKeyHash {
    size_t operator()(const ReplicaKey& key) const {
        size_t h1 = std::hash<TableID>{}(key.table_id);
        size_t h2 = std::hash<ShardID>{}(key.shard_id);
        size_t h3 = std::hash<int32_t>{}(key.replica_index);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

struct NodeAgentConf {
    NodeID node_id;
    std::string ip; // 这个是实际pod的ip和port，不一定是代表开放的ip和port
    int32_t port{-1};

    std::string resource_pool{"default"};
    std::string dc;

    std::string manager_host;
    int32_t manager_port{-1};

    int32_t heartbeat_interval_ms{3000};
    int32_t first_sync_retry_ms{1000};
    int32_t action_worker_count{4};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!node_id.empty(),
                                    "node_id should not empty")
        RETURN_IF_INVALID_CONDITION(!ip.empty(), "ip should not empty")
        RETURN_IF_INVALID_CONDITION(port > 0, "port should > 0")
        RETURN_IF_INVALID_CONDITION(!resource_pool.empty(),
                                    "resource_pool should not empty")
        RETURN_IF_INVALID_CONDITION(!dc.empty(), "dc should not empty")
        RETURN_IF_INVALID_CONDITION(!manager_host.empty(),
                                    "manager_host should not empty")
        RETURN_IF_INVALID_CONDITION(manager_port > 0, "manager_port should > 0")
        RETURN_IF_INVALID_CONDITION(heartbeat_interval_ms > 0,
                                    "heartbeat_interval_ms should > 0")
        RETURN_IF_INVALID_CONDITION(first_sync_retry_ms > 0,
                                    "first_sync_retry_ms should > 0")
        RETURN_IF_INVALID_CONDITION(action_worker_count > 0,
                                    "action_worker_count should > 0")
        return Status::OK();
    }
};

// 下面这三个类用来代表我们在sdm那边的spec和state，
//  在heartbeat这边做一层对于rpc的封装
struct NodeReport {
    Endpoint endpoint;
    // 这里有一个endpoint，因为接触到的场景里面，node的IP和port不代表是他开放给外界去连接的ip和port
    // 所以conf那边的ip和port代表的是node本身的，然后在node_report这边是记录的开放给外界的
    // 这边还留了点，以后可以上传一些资源信息，方便sdm那边调度
};

struct ReplicaReport {
    ReplicaKey key;
    pb::ReplicaRole role{pb::ReplicaRole::FOLLOWER};
    pb::ReplicaStatus status{pb::ReplicaStatus::ADDING};
    Endpoint endpoint;
};

struct DesiredReplicaSpec {
    ReplicaKey key;
    pb::ReplicaRole role{pb::ReplicaRole::FOLLOWER};
    pb::EngineType engine_type{pb::EngineType::ENGINE_MAP};
    bool is_dropped{false};
};

// 这几个是callback用到的

struct CreateReplicaArgs {
    ReplicaKey key;
    pb::ReplicaRole role{pb::ReplicaRole::FOLLOWER};
    pb::EngineType engine_type{pb::EngineType::ENGINE_MAP};
};

struct CreateReplicaResult {
    Endpoint endpoint;
};

struct DeleteReplicaArgs {
    ReplicaKey key;
};

struct ChangeReplicaRoleArgs {
    ReplicaKey key;
    pb::ReplicaRole old_role{pb::ReplicaRole::FOLLOWER};
    pb::ReplicaRole new_role{pb::ReplicaRole::FOLLOWER};
};

struct ChangeReplicaRoleResult {
    Endpoint endpoint;
};

}  // namespace adviskv::sdsdk
