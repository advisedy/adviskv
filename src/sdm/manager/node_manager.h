#pragma once

#include "common.pb.h"
#include "common/status.h"

#include "common/type.h"

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <string>

namespace adviskv{

struct NodeMeta{
    NodeID node_id;
    std::string ip;
    int32_t port;
    std::string zone; // 只有同一个zone的，才可以被放在一起

};

struct NodeStats {
    NodeID node_id;

    int32_t owned_replica_count{0};
    int32_t leader_count{0};
};

struct RegisterNodeParam{
    NodeID node_id;
    std::string ip;
    int32_t port;
    std::string zone;
};
    
struct ListNodesParam{
    std::string zone;
};

// struct FilterBetterNodesParam{
//     int32_t shard_count;
//     int32_t replica_count;
//     std::vector<NodeMeta> nodes;
// };




class NodeManager{

public:
    NodeManager() = default;
    Status add_node(const pb::NodeInfo& node_info);

    Status register_node(const RegisterNodeParam& param, NodeMeta* node_meta);
    Status list_nodes(const ListNodesParam& param, std::vector<NodeMeta>* node_list) const;
    Status list_nodes_stats_by_zone(const std::string& zone, std::vector<NodeStats>* node_list) const;

    Status update_node_owned_replica_count(NodeID node_id, int32_t delta_value);

    Status get_node_meta(const NodeID& node_id, NodeMeta* node_meta) const;
    Status get_node_stats(const NodeID& node_id, NodeStats* node_stats) const;
    Status get_node_list_stats(const std::vector<NodeID>& node_list_id, std::vector<NodeStats>* node_list_stats) const;

    // 返回出来的nodes是分配好shard和replica_index的，第一层是shard_index，第二层是replica_index
    // 然后对于每一个shard里面，默认是replica_index = 0是设置为主节点。
    // Status filter_better_nodes(FilterBetterNodesParam param, std::vector<std::vector<NodeID>>* better_nodes) const;

private:
    mutable std::shared_mutex node_map_mtx_;
    std::unordered_map<NodeID, NodeMeta> node_map_;    
    std::unordered_map<NodeID, NodeStats> node_stats_map_;
};

}