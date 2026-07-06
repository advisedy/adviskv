#include "sdm/handler/sdm_service_impl.h"

#include <cstddef>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "common.pb.h"
#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "sdm.pb.h"
#include "sdm/model/param.h"
#include "sdm/model/model.h"
#include "sdm/service/service_manager.h"

namespace adviskv::sdm {

SdmServiceImpl::SdmServiceImpl(ServiceManager* service_manager) : service_manager_(service_manager) {
}

grpc::Status SdmServiceImpl::PlaceTable(grpc::ServerContext* context, const rpc::PlaceTableRequest* request,
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
    if (!decode_pb_engine_type(request->engine_type(), param.engine_type)) {
        fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, "engine_type is not valid"});
        return grpc::Status::OK;
    }

    Status status = service_manager_->place_table(param);

    fill_base_rsp(response, status);

    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::DropTable(grpc::ServerContext* context, const rpc::DropTableRequest* request,
                                       rpc::DropTableResponse* response) {
    UNUSED(context);
    DropTableParam param;
    param.table_id = request->table_id();
    param.operation_id = request->operation_id();
    Status status = service_manager_->drop_table(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::AlterTableReplicaCount(grpc::ServerContext* context,
                                                    const rpc::AlterTableReplicaCountRequest* request,
                                                    rpc::AlterTableReplicaCountResponse* response) {
    UNUSED(context);

    AlterReplicaCountParam param;
    param.table_id = request->table_id();
    param.replica_count = request->replica_count();
    param.operation_id = request->operation_id();

    Status status = service_manager_->alter_table_replica_count(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::GetTableStatus(grpc::ServerContext* context, const rpc::GetTableStatusRequest* request,
                                            rpc::GetTableStatusResponse* response) {
    UNUSED(context);

    GetTableStatusParam param;
    param.operation_id = request->operation_id();
    param.table_id = request->table_id();

    Table table;
    Status status = service_manager_->get_table_status(param, &table);
    fill_base_rsp(response, status);
    if (status.ok()) {
        pb::SdmTableDesired desired_pb =
            pb::SdmTableDesired::SDM_TABLE_DESIRED_UNSPECIFIED;
        pb::SdmTablePhase phase_pb =
            pb::SdmTablePhase::SDM_TABLE_PHASE_UNSPECIFIED;
        if (!encode_pb_sdm_table_desired(table.state.desired, desired_pb)) {
            fill_base_rsp(response,
                          Status{StatusCode::ERROR, "table desired is not valid"});
            return grpc::Status::OK;
        }
        if (!encode_pb_sdm_table_phase(table.state.phase, phase_pb)) {
            fill_base_rsp(response,
                          Status{StatusCode::ERROR, "table phase is not valid"});
            return grpc::Status::OK;
        }
        response->set_table_id(table.table_id);
        response->set_desired(desired_pb);
        response->set_phase(phase_pb);
        response->set_last_error_msg(table.state.last_error_msg);
        response->set_operation_id(table.spec.operation_id);
    }

    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::Heartbeat(grpc::ServerContext* context, const rpc::HeartbeatRequest* request,
                                       rpc::HeartbeatResponse* response) {
    UNUSED(context);
    std::vector<HeartBeatReplicaInfo> replica_info_list;
    for (const auto& replica_info_pb : request->replica_info_list()) {
        ReplicaRole role = ReplicaRole::FOLLOWER;
        if (!decode_pb_raft_role(replica_info_pb.role(), role)) {
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, "replica role is not valid"});
            return grpc::Status::OK;
        }

        StorageReplicaStatus storage_status = StorageReplicaStatus::INITIALIZING;
        if (!decode_pb_storage_replica_status(replica_info_pb.status(), storage_status)) {
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, "replica status is not valid"});
            return grpc::Status::OK;
        }

        HeartBeatReplicaInfo one;
        if (!decode_pb_replica_id(replica_info_pb.replica_id(),
                                  one.replica_id)) {
            fill_base_rsp(
                response,
                Status{StatusCode::INVALID_ARGUMENT,
                       "replica id is not valid"});
            return grpc::Status::OK;
        }
        one.role = role;
        one.storage_status = storage_status;
        one.term = replica_info_pb.term();
        if (!decode_pb_raft_member_type(replica_info_pb.member_type(), one.member_type)) {
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, "replica member type is not valid"});
            return grpc::Status::OK;
        }
        for (const auto& member_pb : replica_info_pb.full_membership()) {
            RaftMember member;
            if (!decode_pb_raft_member(member_pb, member)) {
                fill_base_rsp(response,
                              Status{StatusCode::INVALID_ARGUMENT, "replica full membership member is not valid"});
                return grpc::Status::OK;
            }
            one.full_membership.push_back(std::move(member));
        }
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
            if (!encode_pb_expected_replica(expect, *expect_pb)) {
                fill_base_rsp(response,
                              Status{StatusCode::ERROR,
                                     "expected replica is not valid"});
                return grpc::Status::OK;
            }
        }
    }

    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::RegisterNode(grpc::ServerContext* context, const rpc::RegisterNodeRequest* request,
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

grpc::Status SdmServiceImpl::GetRoute(grpc::ServerContext* context, const rpc::GetRouteRequest* request,
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
        LOG_WARN("GetRoute failed, db={}, table={}, key={}, status={}", param.db_name, param.table_name, param.key,
                 status.to_string());
        return grpc::Status::OK;
    }
    if (!route.replicas.empty()) {
        response->set_table_id(route.shard_id.table_id);
        response->set_shard_id(route.shard_id.shard_index);
        for (const auto& replica : route.replicas) {
            pb::RaftRole role_pb = pb::RaftRole::RAFT_ROLE_UNSPECIFIED;
            if (!encode_pb_raft_role(replica.role, role_pb)) {
                fill_base_rsp(response,
                              Status{StatusCode::ERROR,
                                     "route replica role is not valid"});
                return grpc::Status::OK;
            }
            auto* route_replica = response->add_replicas();
            encode_pb_replica_id(replica.replica_id, *route_replica->mutable_replica_id());
            auto* endpoint = route_replica->mutable_endpoint();
            endpoint->set_ip(replica.ip);
            endpoint->set_port(replica.port);
            route_replica->set_role(role_pb);
        }
    }
    LOG_INFO(
            "GetRoute ok, db={}, table={}, key={}, table_id={}, shard_id={}, "
            "replicas={}, status={}",
            param.db_name, param.table_name, param.key, route.shard_id.table_id, route.shard_id.shard_index,
            route.replicas.size(), status.to_string());
    return grpc::Status::OK;
}

}  // namespace adviskv::sdm
