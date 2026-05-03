#include "sdm/handler/sdm_service_impl.h"

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <cstddef>

#include "common.pb.h"
#include "common/func.h"
#include "common/status.h"
#include "sdm.pb.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include "sdm/utility/pb_convert.h"

namespace adviskv::sdm {

SdmServiceImpl::SdmServiceImpl(TableService* table_service,
                               NodeService* node_service,
                               HeartBeatService* heartbeat_service,
                               RouteService* route_service)
    : table_service_(table_service),
      node_service_(node_service),
      heartbeat_service_(heartbeat_service),
      route_service_(route_service) {}

grpc::Status SdmServiceImpl::PlaceTable(grpc::ServerContext* context,
                                        const rpc::PlaceTableRequest* request,
                                        rpc::PlaceTableResponse* response) {
    PlaceTableParam param{
        .db_id = request->db_id(),
        .table_id = request->table_id(),
        .db_name = request->db_name(),
        .table_name = request->table_name(),
        .replica_count = request->replica_count(),
        .shard_count = request->shard_count(),
        .resource_pool = request->resource_pool(),
    };

    Status status = table_service_->place_table(param);

    fill_base_rsp(response, status);

    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::PlaceDB(grpc::ServerContext* context,
                                     const rpc::PlaceDBRequest* request,
                                     rpc::PlaceDBResponse* response) {
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::HeartBeat(grpc::ServerContext* context,
                                       const rpc::HeartBeatRequest* request,
                                       rpc::HeartBeatResponse* response) {
    // NodeStatus node_status;
    // CONVERT_PB_TO_NODE_STATUS(request->node_status(), node_status)

    std::vector<HeartBeatReplicaInfo> replica_info_list;
    for (const auto& replica_info : request->replica_info_list()) {
        ReplicaRole role;
        CONVERT_PB_TO_REPLICA_ROLE(replica_info.role(), role)

        ReplicaStatus status;
        CONVERT_PB_TO_REPLICA_STATUS(replica_info.status(), status)

        HeartBeatReplicaInfo one{.shard_id =
                                     ShardID{
                                         .table_id = replica_info.table_id(),
                                         .shard_index = replica_info.shard_id(),
                                     },
                                 .replica_index = replica_info.replica_index(),
                                 .role = role,
                                 .status = status};
        replica_info_list.emplace_back(std::move(one));
    }
    HeartBeatParam param{
        .node_id = request->node_id(),
        .ip = request->ip(),
        .port = request->port(),
        .dc = request->dc(),
        .resoure_pool_name = request->resource_pool(),
        // .status = node_status,
        .replica_list = std::move(replica_info_list),
        .last_heartbeat_ts = adviskv::get_current_ts_ms(),
    };

    HeartBeatResult hb_res;
    Status status = heartbeat_service_->heartbeat(param, hb_res);

    (void)hb_res;
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::RegisterNode(
    grpc::ServerContext* context, const rpc::RegisterNodeRequest* request,
    rpc::RegisterNodeResponse* response) {
    RegisterNodeParam param{
        .node_id = request->node_id(),
        .ip = request->ip(),
        .port = request->port(),
        .resource_pool = request->resource_pool(),
        .dc = request->dc(),
    };
    Status status = node_service_->register_node(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::GetRoute(grpc::ServerContext* context,
                                      const rpc::GetRouteRequest* request,
                                      rpc::GetRouteResponse* response) {
    GetRouteParam param{
        .db_name = request->db_name(),
        .table_name = request->table_name(),
        .key = request->key(),
    };
    ShardRoute route;
    Status status = route_service_->get_route(param, &route);
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    if (!route.replicas.empty()) {
        response->set_table_id(route.shard_id.table_id);
        response->set_shard_id(route.shard_id.shard_index);
        // TODO 这里先暂时找一个pos:0的
        // 感觉实际上我们应该直接把路由表返回了？ 而不是只返回一个ip和port？
        response->set_ip(route.replicas[0].sp);
        response->set_port(route.replicas[0].port);
    }
    return grpc::Status::OK;
}

}  // namespace adviskv::sdm
