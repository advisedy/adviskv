#include "sdm/handler/sdm_service_impl.h"

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <cstddef>

#include "common.pb.h"
#include "common/status.h"
#include "sdm.pb.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include "sdm/utility/pb_convert.h"
#include "common/func.h"

namespace adviskv::sdm {


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

        HeartBeatReplicaInfo one{.table_id = replica_info.table_id(),
                                 .shard_id = replica_info.shard_id(),
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

    //返回给storage侧期望的replica_list
    for(const auto& entry:hb_res.entry_list){
      rpc::HeartBeatReplicaExpect* expect =response->add_replica_list();
      rpc::HeartBeatReplicaExpect& one = *expect;
      one.set_table_id(entry.replica_id.table_id);
      one.set_shard_id(entry.replica_id.shard_id);
      one.set_replica_index(entry.replica_id.replica_index);

      pb::ReplicaRole role;
      CONVERT_REPLICA_ROLE_TO_PB(entry.replica_role, role)
      one.set_role(role);
    }
    fill_base_rsp(response, Status::OK());
    return grpc::Status::OK;
}

}  // namespace adviskv::sdm