#include "sdm/service/placement_service.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/node_manager.h"

namespace adviskv {

Status PlacementService::place_table(const PlaceTableParam &param,
                                     TableMetaCache *table_meta_cache) {
  // TODO
  // 首先列出来所有可以用的node，然后择优选择出来最终的nodes
  // 然后给这些node要求它们去创建replica，最后更新route_manager里的TableMetaCache
  std::vector<NodeMeta> node_list;
  ListNodesParam list_nodes_param{.zone = param.zone};
  Status status = node_manager_->list_nodes(list_nodes_param, &node_list);
  RETURN_IF_INVALID_STATUS(status)

  std::vector<std::vector<NodeID>> better_nodes;
  FilterBetterNodesParam filter_param{.shard_count = param.shard_count,
                                      .replica_count = param.replica_count,
                                      .nodes = node_list};
  status = node_manager_->filter_better_nodes(filter_param, &better_nodes);
  RETURN_IF_INVALID_STATUS(status)

  // 调用storage的接口，要求这些node去创建replica

  for (const std::vector<NodeID> &replica_nodes : better_nodes) {
    for (const NodeID &node_id : replica_nodes) {
      // TODO
      // 调用storage的接口，要求node_id去创建replica，参数里应该还要带上table_id和shard_id
    }
  }

  for (const std::vector<NodeID> &replica_nodes : better_nodes) {
    for (const NodeID &node_id : replica_nodes) {
      // TODO
      Status status =
          node_manager_->update_node_owned_replica_count(node_id, 1);
      RETURN_IF_INVALID_STATUS(status)
    }
  }
}

} // namespace adviskv