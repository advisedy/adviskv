#include "sdm/service/placement_service.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/meta_cache_manager.h"
#include "sdm/manager/node_manager.h"
#include "sdm/manager/route_manager.h"

namespace adviskv {

Status PlacementService::place_table(const PlaceTableParam &param,
                                     TableMetaCache *table_meta_cache) {

  // 首先列出来所有可以用的node，然后择优选择出来最终的nodes
  // 然后给这些node要求它们去创建replica，最后更新route_manager里的TableMetaCache

  RETURN_IF_INVALID_PARAM(param)
                                        DBMetaCache db_meta;
  Status status = meta_cache_manager_->get_db_meta(param.db_name, &db_meta);
    RETURN_IF_INVALID_STATUS(status)

  std::vector<NodeStats> node_list;

  status = node_manager_->list_nodes_stats_by_zone(db_meta.zone, &node_list);
  RETURN_IF_INVALID_STATUS(status)

    NodeSelectorParam selector_param{.shard_count = param.shard_count,
                                    .replica_count = param.replica_count,
                                    .nodes = std::move(node_list)};
    std::vector<std::vector<NodeID>> final_nodes;
    status = node_selector_->select_nodes(selector_param, &final_nodes);

  RETURN_IF_INVALID_STATUS(status)

    std::vector<NodeID> leaders;
    leaders.reserve(final_nodes.size());
    for (const std::vector<NodeID> &replica_nodes : final_nodes) {
        NodeID leader;
        std::vector<NodeStats> replica_node_stats;
        replica_node_stats.reserve(replica_nodes.size());
        status = node_manager_->get_node_list_stats(replica_nodes, &replica_node_stats);
        RETURN_IF_INVALID_STATUS(status)
        status = leader_selector_->select_leader(replica_node_stats, leader);
        RETURN_IF_INVALID_STATUS(status)
        leaders.emplace_back(leader);
    }

  // 调用storage的接口，要求这些node去创建replica

  for (const std::vector<NodeID> &replica_nodes : final_nodes) {
    for (const NodeID &node_id : replica_nodes) {
      // TODO
      // 调用storage的接口，要求node_id去创建replica，参数里应该还要带上table_id和shard_id
      // leaders 这里现在还没有用上，传给storage那边的时候记得得带上是否是leader的信息
    }
  }

  status = meta_cache_manager_->update_table_meta(TableMetaCache{
                                    .db_name = param.db_name,
                                        .table_name = param.table_name,
                                      .table_id = param.table_id,
                                      .db_id = param.db_id,
                                      .shard_count = param.shard_count,
                                      .replica_count = param.replica_count});

  RETURN_IF_INVALID_STATUS(status)
//TODO  这里如果失败了，得回滚

  for (const std::vector<NodeID> &replica_nodes : final_nodes) {
    for (const NodeID &node_id : replica_nodes) {
      Status status =
          node_manager_->update_node_owned_replica_count(node_id, 1);
      RETURN_IF_INVALID_STATUS(status)
    }
  }

  //TODO 更新route到route_manager里，后续补上

  for(int i=0;i<final_nodes.size(); i++){
    ShardRoute route;
    route.table_id = param.table_id;
    route.shard_id = i;

    for(int j= 0;j<final_nodes[i].size(); j++){
        ReplicaLocation replica_location;
        replica_location.replica_index = j;
        replica_location.node_id = final_nodes[i][j];
        replica_location.role = (final_nodes[i][j] == leaders[i]) ? ReplicaRole::LEADER : ReplicaRole::FOLLOWER;
        NodeMeta node_meta;
        status = node_manager_->get_node_meta(final_nodes[i][j], &node_meta);
        replica_location.ip = node_meta.ip;
        replica_location.port = node_meta.port;
        RETURN_IF_INVALID_STATUS(status)
        //TODO  这里如果失败了，得回滚

        route.replicas.emplace_back(replica_location);
    }

    status = route_manager_->update_route(route);
    RETURN_IF_INVALID_STATUS(status)
  }

  return Status::OK();
}

} // namespace adviskv