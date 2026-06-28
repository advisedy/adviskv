#include "sdm/handler/sdm_service_impl.h"

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <cstddef>

#include "common.pb.h"
#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/proto/expected_replica_proto.h"
#include "common/proto/raft_role_proto.h"
#include "common/proto/replica_id_proto.h"
#include "common/proto/sdm_table_status_proto.h"
#include "common/proto/storage_replica_status_proto.h"
#include "common/status.h"
#include "sdm.pb.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include "sdm/service/service_manager.h"

namespace adviskv::sdm {

SdmServiceImpl::SdmServiceImpl(ServiceManager* service_manager)
    : service_manager_(service_manager) {}

grpc::Status SdmServiceImpl::PlaceTable(grpc::ServerContext* context,
                                        const rpc::PlaceTableRequest* request,
                                        rpc::PlaceTableResponse* response) {
    UNUSED(context);

    PlaceTableParam param;
    param.db_id = request->db_id();
    param.table_id = request->table_id();
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.replica_count = request->replica_count();
    param.shard_count = request->shard_count();
    param.resource_pool = request->resource_pool();
    param.operation_id = request->operation_id();

    Status status = service_manager_->place_table(param);

    fill_base_rsp(response, status);

    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::DropTable(grpc::ServerContext* context,
                                       const rpc::DropTableRequest* request,
                                       rpc::DropTableResponse* response) {
    UNUSED(context);
    DropTableParam param;
    param.table_id = request->table_id();
    param.operation_id = request->operation_id();
    Status status = service_manager_->drop_table(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::AlterTableReplicaCount(
    grpc::ServerContext* context,
    const rpc::SdmAlterTableReplicaCountRequest* request,
    rpc::SdmAlterTableReplicaCountResponse* response) {
    UNUSED(context);

    AlterReplicaCountParam param;
    param.table_id = request->table_id();
    param.replica_count = request->replica_count();
    param.operation_id = request->operation_id();

    Status status = service_manager_->alter_table_replica_count(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::GetTableStatus(
    grpc::ServerContext* context, const rpc::GetTableStatusRequest* request,
    rpc::GetTableStatusResponse* response) {
    UNUSED(context);

    GetTableStatusParam param;
    param.operation_id = request->operation_id();
    param.table_id = request->table_id();

    Table table;
    Status status = service_manager_->get_table_status(param, &table);
    fill_base_rsp(response, status);
    if (status.ok()) {
        response->set_table_id(table.table_id);
        response->set_desired(to_pb_sdm_table_desired(table.state.desired));
        response->set_phase(to_pb_sdm_table_phase(table.state.phase));
        response->set_last_error_msg(table.state.last_error_msg);
        response->set_operation_id(table.spec.operation_id);
    }

    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::HeartBeat(grpc::ServerContext* context,
                                       const rpc::HeartBeatRequest* request,
                                       rpc::HeartBeatResponse* response) {
    UNUSED(context);
    std::vector<HeartBeatReplicaInfo> replica_info_list;
    for (const auto& replica_info : request->replica_info_list()) {
        ReplicaRole role = ReplicaRole::FOLLOWER;
        if (!decode_pb_raft_role(replica_info.role(), role)) {
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT,
                                           "replica role is not valid"});
            return grpc::Status::OK;
        }

        StorageReplicaStatus storage_status =
            StorageReplicaStatus::INITIALIZING;
        if (!decode_pb_storage_replica_status(replica_info.status(),
                                              storage_status)) {
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT,
                                           "replica status is not valid"});
            return grpc::Status::OK;
        }

        HeartBeatReplicaInfo one;
        one.replica_id = decode_pb_replica_id(replica_info.replica_id());
        one.role = role;
        one.storage_status = storage_status;
        one.term = replica_info.term();
        replica_info_list.emplace_back(std::move(one));
    }
    HeartBeatParam param;
    param.node_id = request->node_id();
    param.ip = request->ip();
    param.port = request->port();
    param.resoure_pool_name = request->resource_pool();
    param.dc = request->dc();
    param.replica_list = std::move(replica_info_list);
    param.last_heartbeat_ts = func::get_current_ts_ms();

    HeartBeatResult result;
    Status status = service_manager_->heartbeat(param, &result);
    if (status.ok()) {
        for (const ExpectedReplica& expect : result.expects) {
            auto* expect_pb = response->add_expects();
            encode_pb_expected_replica(expect, *expect_pb);
        }
    }

    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::RegisterNode(
    grpc::ServerContext* context, const rpc::RegisterNodeRequest* request,
    rpc::RegisterNodeResponse* response) {
    UNUSED(context);
    RegisterNodeParam param;
    param.node_id = request->node_id();
    param.ip = request->ip();
    param.port = request->port();
    param.resource_pool = request->resource_pool();
    param.dc = request->dc();
    param.last_heartbeat_ts = func::get_current_ts_ms();
    Status status = service_manager_->register_node(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::GetRoute(grpc::ServerContext* context,
                                      const rpc::GetRouteRequest* request,
                                      rpc::GetRouteResponse* response) {
    UNUSED(context);
    GetRouteParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.key = request->key();
    ShardRoute route;
    Status status = service_manager_->get_route(param, &route);
    fill_base_rsp(response, status);
    if (status.fail()) {
        LOG_WARN("GetRoute failed, db={}, table={}, key={}, status={}",
                 param.db_name, param.table_name, param.key,
                 status.to_string());
        return grpc::Status::OK;
    }
    if (!route.replicas.empty()) {
        response->set_table_id(route.shard_id.table_id);
        response->set_shard_id(route.shard_id.shard_index);
        for (const auto& replica : route.replicas) {
            auto* route_replica = response->add_replicas();
            encode_pb_replica_id(replica.replica_id,
                                 *route_replica->mutable_replica_id());
            auto* endpoint = route_replica->mutable_endpoint();
            endpoint->set_ip(replica.ip);
            endpoint->set_port(replica.port);
            route_replica->set_role(to_pb_raft_role(replica.role));
        }
    }
    LOG_INFO(
        "GetRoute ok, db={}, table={}, key={}, table_id={}, shard_id={}, "
        "replicas={}, status={}",
        param.db_name, param.table_name, param.key, route.shard_id.table_id,
        route.shard_id.shard_index, route.replicas.size(), status.to_string());
    return grpc::Status::OK;
}

}  // namespace adviskv::sdm