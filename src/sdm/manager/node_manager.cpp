#include "sdm/manager/node_manager.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include <algorithm>
#include <cstdint>
#include <mutex>
#include <vector>

namespace adviskv {

Status NodeManager::add_node(const pb::NodeInfo &node_info) {
  // if(node_map_.count(node_info.node_name())){
  //     return Status{StatusCode::INVALID_ARGUMENT, "node_id already exists"};
  // }
  // node_map_[node_info.node_name()] = node_info;
  return Status::OK();
}

Status NodeManager::register_node(const RegisterNodeParam &param,
                                  NodeMeta *node_meta) {
  std::unique_lock<std::shared_mutex> locker(node_map_mtx_);
  if (node_map_.count(param.node_id)) {
    return Status{StatusCode::INVALID_ARGUMENT, "node_id already exists"};
  }
  NodeMeta meta{.node_id = param.node_id,
                .ip = param.ip,
                .port = param.port,
                .owned_replica_count = 0};
  node_map_[param.node_id] = meta;
  if (node_meta) {
    *node_meta = meta;
  }
  return Status::OK();
}

Status NodeManager::list_nodes(const ListNodesParam& param, std::vector<NodeMeta>* node_list) const{
    std::shared_lock<std::shared_mutex> lock(node_map_mtx_);
    // TODO 这里以后可以优化一下，直接维护一个zone到node的映射，这样查询的时候就不需要遍历整个node_map了
    for(const auto& [node_id, node_meta] : node_map_){
        if(param.zone.empty() || node_meta.zone == param.zone){
            node_list->emplace_back(node_meta);
        }
    }
    return Status::OK();
}



Status NodeManager::filter_better_nodes(
    FilterBetterNodesParam param,
    std::vector<std::vector<NodeID>> *better_nodes) const {
  int32_t shard_count = param.shard_count;
  int32_t replica_count = param.replica_count;
  std::vector<NodeMeta> &nodes = param.nodes;

  RETURN_IF_INVALID_CONDITION(
      nodes.size() >= replica_count,
      "node count should be greater than or equal to replica count")
  RETURN_IF_INVALID_CONDITION(better_nodes != nullptr,
                              "better_nodes should not be nullptr")

  for (int i = 0; i < shard_count; i++) {
    std::sort(nodes.begin(), nodes.end(),
              [](const NodeMeta &a, const NodeMeta &b) {
                return a.owned_replica_count < b.owned_replica_count;
              });
    std::vector<NodeID> replica_nodes;
    replica_nodes.reserve(replica_count);
    for (int j = 0; j < replica_count; j++) {
      replica_nodes.emplace_back(nodes[j].node_id);
      nodes[j].owned_replica_count++;
    }
    better_nodes->emplace_back(std::move(replica_nodes));
  }

  return Status::OK();
}

Status NodeManager::update_node_owned_replica_count(NodeID node_id, int32_t delta_value){
    std::unique_lock<std::shared_mutex> lock(node_map_mtx_);
    auto it = node_map_.find(node_id);
    if(it == node_map_.end()){
        return Status{StatusCode::REPLICA_MANAGER_NOT_FOUND, fmt::format("node_id: {} not found", node_id)};
    }
    it->second.owned_replica_count += delta_value;
    return Status::OK();
}



} // namespace adviskv