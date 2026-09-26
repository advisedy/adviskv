#include "meta/handler/meta_service_impl.h"

#include <vector>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "common/types.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/proto/table_phase_proto.h"
#include "meta/service/service_manager.h"

namespace adviskv::meta {

MetaServiceImpl::MetaServiceImpl(ServiceManager* service_manager) : service_manager_(service_manager) {}

grpc::Status MetaServiceImpl::CreateDB(grpc::ServerContext* context, const meta_rpc::CreateDBRequest* request,
                                       meta_rpc::CreateDBResponse* response) {
    UNUSED(context);
    CreateDBParam param{request->db_name(), request->zone()};
    Database db;
    Status status = service_manager_->create_db(param, &db);
    fill_base_rsp(response, status);
    if (status.ok()) response->set_db_id(db.db_id);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::DropDB(grpc::ServerContext* context, const meta_rpc::DropDBRequest* request,
                                     meta_rpc::DropDBResponse* response) {
    UNUSED(context);
    DropDBParam param{request->db_name()};
    Database db;
    Status status = service_manager_->drop_db(param, &db);
    fill_base_rsp(response, status);
    if (status.ok()) response->set_db_id(db.db_id);
    return grpc::Status::OK;
}

namespace {
template <typename Response>
void fill_table_rsp_ids_and_phase(Response* response, const Table& table) {
    pb::TablePhase phase_pb;
    if (!encode_pb_table_phase(table.state.phase, phase_pb)) {
        fill_base_rsp(response, Status::ERROR("table phase is not valid"));
        return;
    }
    response->set_table_id(table.table_id);
    response->set_phase(phase_pb);
    response->set_operation_id(table.state.operation_id);
}
}  // namespace

grpc::Status MetaServiceImpl::CreateTable(grpc::ServerContext* context, const meta_rpc::CreateTableRequest* request,
                                          meta_rpc::CreateTableResponse* response) {
    UNUSED(context);
    CreateTableParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.shard_count = request->shard_count();
    param.replica_count = request->replica_count();
    param.resource_pool = request->resource_pool();
    if (!decode_pb_engine_type(request->engine_type(), param.engine_type)) {
        fill_base_rsp(response, Status::INVALID_ARGUMENT("engine_type is not valid"));
        return grpc::Status::OK;
    }
    Table table;
    Status status = service_manager_->create_table(param, &table);
    fill_base_rsp(response, status);
    if (status.ok()) fill_table_rsp_ids_and_phase(response, table);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::DropTable(grpc::ServerContext* context, const meta_rpc::DropTableRequest* request,
                                        meta_rpc::DropTableResponse* response) {
    UNUSED(context);
    DropTableByNameParam param{request->db_name(), request->table_name()};
    Table table;
    Status status = service_manager_->drop_table(param, &table);
    fill_base_rsp(response, status);
    if (status.ok()) fill_table_rsp_ids_and_phase(response, table);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::AlterTableReplicaCount(grpc::ServerContext* context,
                                                     const meta_rpc::AlterTableReplicaCountRequest* request,
                                                     meta_rpc::AlterTableReplicaCountResponse* response) {
    UNUSED(context);
    AlterTableReplicaCountParam param{request->db_name(), request->table_name(), request->replica_count()};
    Table table;
    Status status = service_manager_->alter_table_replica_count(param, &table);
    fill_base_rsp(response, status);
    if (status.ok()) fill_table_rsp_ids_and_phase(response, table);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::GetTable(grpc::ServerContext* context, const meta_rpc::GetTableRequest* request,
                                       meta_rpc::GetTableResponse* response) {
    UNUSED(context);
    GetTableParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.use_table_id = request->use_table_id();
    param.table_id = request->table_id();
    Table table;
    Status status = service_manager_->get_table(param, &table);
    fill_base_rsp(response, status);
    if (status.fail()) return grpc::Status::OK;
    pb::TablePhase phase_pb;
    pb::EngineType engine_type_pb;
    if (!encode_pb_table_phase(table.state.phase, phase_pb) ||
        !encode_pb_engine_type(table.spec.engine_type, engine_type_pb)) {
        fill_base_rsp(response, Status::ERROR("table fields are not valid"));
        return grpc::Status::OK;
    }
    response->set_db_id(table.spec.db_id);
    response->set_table_id(table.table_id);
    response->set_replica_count(table.spec.target_replica_count);
    response->set_shard_count(table.shard_count());
    response->set_phase(phase_pb);
    response->set_last_error_msg(table.state.last_error_msg);
    response->set_operation_id(table.state.operation_id);
    response->set_engine_type(engine_type_pb);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::Heartbeat(grpc::ServerContext* context, const meta_rpc::HeartbeatRequest* request,
                                        meta_rpc::HeartbeatResponse* response) {
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
        if (!decode_pb_replica_id(replica_info_pb.replica_id(), one.replica_id)) {
            fill_base_rsp(response, Status{StatusCode::INVALID_ARGUMENT, "replica id is not valid"});
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

    HeartBeatResult result;
    Status status = service_manager_->heartbeat(param, &result);
    if (status.ok()) {
        for (const ExpectedReplica& expect : result.expects) {
            auto* expect_pb = response->add_expects();
            if (!encode_pb_expected_replica(expect, *expect_pb)) {
                fill_base_rsp(response, Status{StatusCode::ERROR, "expected replica is not valid"});
                return grpc::Status::OK;
            }
        }
    }

    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status MetaServiceImpl::RegisterNode(grpc::ServerContext* context, const meta_rpc::RegisterNodeRequest* request,
                                           meta_rpc::RegisterNodeResponse* response) {
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

grpc::Status MetaServiceImpl::GetTableRoutes(grpc::ServerContext* context,
                                             const meta_rpc::GetTableRoutesRequest* request,
                                             meta_rpc::GetTableRoutesResponse* response) {
    UNUSED(context);

    GetTableRoutesParam param;
    param.db_name = request->db_name();
    param.table_name = request->table_name();
    param.include_unavailable = request->include_unavailable();

    Table table;
    std::vector<ShardRoute> routes;
    Status status = service_manager_->get_table_routes(param, &table, &routes);
    fill_base_rsp(response, status);
    if (status.fail()) {
        LOG_WARN("[MetaServiceImpl] GetTableRoutes failed, db={}, table={}, status={}", param.db_name, param.table_name,
                 status.to_string());
        return grpc::Status::OK;
    }

    response->set_table_id(table.table_id);
    response->set_shard_count(table.shard_count());
    for (const auto& route : routes) {
        auto* route_pb = response->add_routes();
        route_pb->set_shard_id(route.shard_id.shard_index);
        route_pb->mutable_hash_range()->set_start(route.hash_range.start);
        route_pb->mutable_hash_range()->set_end(route.hash_range.end);
        for (const auto& replica : route.replicas) {
            pb::RaftRole role_pb;
            if (!encode_pb_raft_role(replica.role, role_pb)) {
                fill_base_rsp(response, Status{StatusCode::ERROR, "route replica role is not valid"});
                return grpc::Status::OK;
            }
            auto* route_replica = route_pb->add_replicas();
            encode_pb_replica_id(replica.replica_id, *route_replica->mutable_replica_id());
            auto* endpoint = route_replica->mutable_endpoint();
            endpoint->set_ip(replica.ip);
            endpoint->set_port(replica.port);
            route_replica->set_role(role_pb);
        }
    }

    LOG_INFO("[MetaServiceImpl] GetTableRoutes ok, db={}, table={}, table_id={}, shard_count={}, routes={}",
             param.db_name, param.table_name, table.table_id, table.shard_count(), routes.size());
    return grpc::Status::OK;
}

}  // namespace adviskv::meta
