// #include "sdm/manager/node_manager.h"
// #include "common/define.h"
// #include "common/status.h"
// #include "common/type.h"
// #include <algorithm>
// #include <cstddef>
// #include <cstdint>
// #include <mutex>
// #include <vector>

// namespace adviskv {

// Status NodeManager::register_node(const NodeID &node_id, const std::string &ip,
//                                   int32_t port, const std::string &zone,
//                                   NodeMeta *node_meta) {
//   std::unique_lock<std::shared_mutex> locker(node_map_mtx_);
//   if (node_map_.count(node_id)) {
//     return Status{StatusCode::INVALID_ARGUMENT, "node_id already exists"};
//   }
//   NodeMeta meta{.node_id = node_id, .ip = ip, .port = port, .zone = zone};
//   node_map_[node_id] = meta;

//   NodeStats stats{
//       .node_id = node_id, .owned_replica_count = 0, .leader_count = 0};
//   node_stats_map_[node_id] = stats;

//   if (node_meta) {
//     *node_meta = meta;
//   }
//   return Status::OK();
// }

// Status NodeManager::list_nodes(const std::string &zone,
//                                std::vector<NodeMeta> *node_list) const {
//   std::shared_lock<std::shared_mutex> lock(node_map_mtx_);
//   // TODO
//   // 这里以后可以优化一下，直接维护一个zone到node的映射，这样查询的时候就不需要遍历整个node_map了
//   for (const auto &[node_id, node_meta] : node_map_) {
//     if (zone.empty() || node_meta.zone == zone) {
//       node_list->emplace_back(node_meta);
//     }
//   }
//   return Status::OK();
// }

// Status
// NodeManager::list_nodes_stats_by_zone(const std::string &zone,
//                                       std::vector<NodeStats> *node_list) const {
//   std::shared_lock<std::shared_mutex> lock(node_map_mtx_);

//   for (const auto &[node_id, node_stats] : node_stats_map_) {
//     if (zone.empty() || node_map_.at(node_id).zone == zone) {
//       node_list->emplace_back(node_stats);
//     }
//   }
//   return Status::OK();
// }

// // Status NodeManager::filter_better_nodes(
// //     FilterBetterNodesParam param,
// //     std::vector<std::vector<NodeID>> *better_nodes) const {
// //   int32_t shard_count = param.shard_count;
// //   int32_t replica_count = param.replica_count;
// //   std::vector<NodeMeta> &nodes = param.nodes;

// //   RETURN_IF_INVALID_CONDITION(
// //       nodes.size() >= replica_count,
// //       "node count should be greater than or equal to replica count")
// //   RETURN_IF_INVALID_CONDITION(better_nodes != nullptr,
// //                               "better_nodes should not be nullptr")

// //   for (int i = 0; i < shard_count; i++) {
// //     std::sort(nodes.begin(), nodes.end(),
// //               [](const NodeMeta &a, const NodeMeta &b) {
// //                 return a.owned_replica_count < b.owned_replica_count;
// //               });
// //     std::vector<NodeID> replica_nodes;
// //     replica_nodes.reserve(replica_count);
// //     for (int j = 0; j < replica_count; j++) {
// //       replica_nodes.emplace_back(nodes[j].node_id);
// //       nodes[j].owned_replica_count++;
// //     }
// //     better_nodes->emplace_back(std::move(replica_nodes));
// //   }

// //   return Status::OK();
// // }

// Status NodeManager::update_node_owned_replica_count(NodeID node_id,
//                                                     int32_t delta_value) {
//   std::unique_lock<std::shared_mutex> lock(node_map_mtx_);
//   auto it = node_stats_map_.find(node_id);
//   if (it == node_stats_map_.end()) {
//     return Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
//                   fmt::format("node_id: {} not found", node_id)};
//   }
//   it->second.owned_replica_count += delta_value;
//   return Status::OK();
// }

// Status NodeManager::get_node_meta(const NodeID &node_id,
//                                   NodeMeta *node_meta) const {
//   std::shared_lock<std::shared_mutex> lock(node_map_mtx_);
//   auto it = node_map_.find(node_id);
//   if (it == node_map_.end()) {
//     return Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
//                   fmt::format("node_id: {} not found", node_id)};
//   }
//   *node_meta = it->second;
//   return Status::OK();
// }

// Status NodeManager::get_node_stats(const NodeID &node_id,
//                                    NodeStats *node_stats) const {
//   std::shared_lock<std::shared_mutex> lock(node_map_mtx_);
//   auto it = node_stats_map_.find(node_id);
//   if (it == node_stats_map_.end()) {
//     return Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
//                   fmt::format("node_id: {} not found", node_id)};
//   }
//   *node_stats = it->second;
//   return Status::OK();
// }

// Status NodeManager::get_node_list_stats(
//     const std::vector<NodeID> &node_list_id,
//     std::vector<NodeStats> *node_list_stats) const {
//   RETURN_IF_INVALID_CONDITION(node_list_stats != nullptr,
//                               "node_list_stats should not be nullptr")
//   std::shared_lock<std::shared_mutex> lock(node_map_mtx_);
//   for (const auto &node_id : node_list_id) {
//     auto it = node_stats_map_.find(node_id);
//     if (it == node_stats_map_.end()) {
//       return Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
//                     fmt::format("node_id: {} not found", node_id)};
//     }
//     node_list_stats->emplace_back(it->second);
//   }
//   return Status::OK();
// }

// } // namespace adviskv