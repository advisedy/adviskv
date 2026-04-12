#include "sdm/operation/placetable_operation.h"
#include "common/define.h"
#include "common/status.h"
#include "sdm/model/route_model.h"
#include "sdm/plan/placement_plan.h"

#include <vector>

namespace adviskv {

// Status execute() override;
// std::string get_name() override;

std::string PlaceTableOperation::get_name() const {
  return "PlaceTableOperation";
}

Status PlaceTableOperation::execute() {
    RETURN_IF_INVALID_PARAM(param_)
  PlaceTablePlan plan;
  Status status = build_plan(plan);
  RETURN_IF_INVALID_STATUS(status)
  RETURN_IF_INVALID_PLAN(plan)
  status = commit_plan(plan);
  return status;
}

Status PlaceTableOperation::build_plan(PlaceTablePlan &plan) {


  // 首先列出来所有可以用的node，然后择优选择出来最终的nodes
  // 然后给这些node要求它们去创建replica，最后更新route_manager里的TableMetaCache

  plan.db_id = param_.db_id;
  plan.db_name = param_.db_name;
  plan.table_id = param_.table_id;
  plan.table_name = param_.table_name;
  plan.shard_count = param_.shard_count;
  plan.replica_count = param_.replica_count;

  // 先筛选出来这个nodes，也就是各个shard里面所放的node
  DBMetaCache db_meta;
  Status status =
      deps_.meta_cache_manager->get_db_meta(param_.db_name, &db_meta);
  RETURN_IF_INVALID_STATUS(status)

  std::vector<NodeStats> node_list;
  status =
      deps_.node_manager->list_nodes_stats_by_zone(db_meta.zone, &node_list);
  RETURN_IF_INVALID_STATUS(status)

  NodeSelectorParam selector_param{.shard_count = param_.shard_count,
                                   .replica_count = param_.replica_count,
                                   .nodes = std::move(node_list)};
  std::vector<std::vector<NodeID>> final_nodes;
  status = deps_.node_selector->select_nodes(selector_param, &final_nodes);

  RETURN_IF_INVALID_STATUS(status)

  int shard_count = final_nodes.size();
  for (int i = 0; i < shard_count; i++) {
    const std::vector<NodeID> &replica_nodes = final_nodes[i];
    // 选择出来leader
    std::vector<NodeStats> replica_node_stats;
    replica_node_stats.reserve(replica_nodes.size());
    status = deps_.node_manager->get_node_list_stats(replica_nodes,
                                                     &replica_node_stats);
    RETURN_IF_INVALID_STATUS(status)
    NodeID leader_node_id;
    status = deps_.leader_selector->select_leader(replica_node_stats,
                                                  leader_node_id);
    RETURN_IF_INVALID_STATUS(status)

    // 开始构造当前这个shard的placement
    ShardPlacement shard_placement;
    shard_placement.table_id = param_.table_id;
    shard_placement.shard_id = i;

    for (int j = 0; j < static_cast<int>(replica_nodes.size()); ++j) {
      const NodeID &node_id = replica_nodes[j];

      NodeMeta node_meta;
      status = deps_.node_manager->get_node_meta(node_id, &node_meta);
      RETURN_IF_INVALID_STATUS(status)

      ReplicaPlacement replica_placement;
      replica_placement.replica_index = j;
      replica_placement.node_id = node_id;
      replica_placement.ip = node_meta.ip;
      replica_placement.port = node_meta.port;
      replica_placement.role = (node_id == leader_node_id)
                                   ? ReplicaRole::LEADER
                                   : ReplicaRole::FOLLOWER;

      shard_placement.replicas.emplace_back(std::move(replica_placement));
    }
    plan.shard_placements.emplace_back(std::move(shard_placement));
  }



  return Status::OK();
}

Status PlaceTableOperation::commit_plan(const PlaceTablePlan &plan) {



  Status status = deps_.meta_cache_manager->update_table_meta(
      TableMetaCache{.db_name = plan.db_name,
                     .table_name = plan.table_name,
                     .table_id = plan.table_id,
                     .db_id = plan.db_id,
                     .shard_count = plan.shard_count,
                     .replica_count = plan.replica_count});

  RETURN_IF_INVALID_STATUS(status)
  // TODO  这里如果失败了，得回滚

  for (const ShardPlacement &shard_placement : plan.shard_placements) {
    for (const ReplicaPlacement &replica_placement : shard_placement.replicas) {
      Status status = deps_.node_manager->update_node_owned_replica_count(
          replica_placement.node_id, 1);
      RETURN_IF_INVALID_STATUS(status)
      // TODO  这里如果失败了，得回滚
    }
  }

  for (int i = 0, i_lim = plan.shard_placements.size(); i < i_lim; i++) {
    ShardRoute route;
    route.table_id = plan.table_id;
    route.shard_id = i;
    for (int j = 0, j_lim = plan.shard_placements[i].replicas.size(); j < j_lim; j++) {
      ReplicaLocation replica_location;
      replica_location.replica_index = j;
      const ReplicaPlacement &replica_placement =
          plan.shard_placements[i].replicas[j];
      replica_location.node_id = replica_placement.node_id;
      replica_location.role = replica_placement.role;
      replica_location.ip = replica_placement.ip;
      replica_location.port = replica_placement.port;

      route.replicas.emplace_back(replica_location);
    }
    status = deps_.route_manager->update_route(route);
    RETURN_IF_INVALID_STATUS(status)
  }

  // TODO:
  // for (const auto& shard : plan.shard_placements) {
  //     for (const auto& replica : shard.replicas) {
  //         call storage create replica
  //     }
  // }

  return Status::OK();
}

} // namespace adviskv