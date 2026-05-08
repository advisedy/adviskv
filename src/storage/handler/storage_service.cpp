#include "storage/handler/storage_service.h"

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "common/confmgr.h"
#include "common/log.h"
#include "common/path_util.h"
#include "common/status.h"
#include "common/type.h"
#include "storage.pb.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

grpc::Status StorageServiceImpl::Put(grpc::ServerContext* context,
                                     const rpc::PutRequest* request,
                                     rpc::PutResponse* response) {
    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    const ShardID shard_id{
        .table_id = request->table_id(),
        .shard_index = request->shard_id(),
    };
    Replica* replica = replica_manager_->get_replica_by_shard(shard_id);

    if (!replica) {
        LOG_WARN("replica not found, table_id = {}, shard_id = {}",
                 request->table_id(), request->shard_id());
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "replica not found"});
        return grpc::Status::OK;
    }
    PutParam param{.key = request->key(), .value = request->value()};
    Status status = replica->put(param);

    if (!status.ok()) {
        LOG_WARN(
            "replica put failed, table_id = {}, shard_id = {}, key = {}, value "
            "= {}, msg = {}",
            request->table_id(), request->shard_id(), request->key(),
            request->value(), status.msg());
    }

    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::Get(grpc::ServerContext* context,
                                     const rpc::GetRequest* request,
                                     rpc::GetResponse* response) {
    const ShardID shard_id{
        .table_id = request->table_id(),
        .shard_index = request->shard_id(),
    };
    Replica* replica = replica_manager_->get_replica_by_shard(shard_id);

    if (!replica) {
        LOG_WARN("replica not found, table_id = {}, shard_id = {}",
                 request->table_id(), request->shard_id());
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "replica not found"});
        return grpc::Status::OK;
    }

    GetParam param{.key = request->key()};
    Value value;

    Status status = replica->get(param, value);

    if (!status.ok()) {
        LOG_WARN(
            "replica get failed, table_id = {}, shard_id = {}, key = {}, msg = "
            "{}",
            request->table_id(), request->shard_id(), request->key(),
            status.msg());
    }
    fill_base_rsp(response, status);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    response->set_value(value);

    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::Delete(grpc::ServerContext* context,
                                        const rpc::DeleteRequest* request,
                                        rpc::DeleteResponse* response) {
    // Replica* replica = replica_manager_->get_replica(request->table_id(),
    // request->shard_id());

    // if(!replica){
    //     LOG_WARN("replica not found, table_id = {}, shard_id = {}",
    //     request->table_id(), request->shard_id()); fill_base_rsp(*response,
    //     Status{StatusCode::REPLICA_NOT_FOUND, "replica not found"}); return
    //     grpc::Status::OK;
    // }

    // Status status = replica->del(*request);

    // if(!status.ok()){
    //     LOG_WARN("replica del failed, table_id = {}, shard_id = {}, key = {},
    //     msg = {}",
    //         request->table_id(), request->shard_id(), request->key(),
    //         status.msg());
    // }

    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::CreateReplica(
    grpc::ServerContext* context, const rpc::CreateReplicaRequest* request,
    rpc::CreateReplicaResponse* response) {
    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    const ShardID shard_id{
        .table_id = request->table_id(),
        .shard_index = request->shard_index(),
    };
    Replica* replica = replica_manager_->get_replica_by_shard(shard_id);

    if (replica) {
        LOG_WARN("replica have exists, table_id = {}, shard_index = {}",
                 request->table_id(), request->shard_index());
        fill_base_rsp(response, Status::ALREADY_EXIST("replica not found"));
        return grpc::Status::OK;
    }

    ReplicaInitParam param{
        .replica_id{.table_id = request->table_id(),
                    .shard_index = request->shard_index(),
                    .replica_index = request->replica_index()},
        .engine_type = (EngineType)request->engine_type(),
    };
    param.members.clear();
    for (const auto& member : request->members()) {
        PeerMember one{
            .node_id = member.node_id(),
            .replica_id{.table_id = member.replica_id().table_id(),
                        .shard_index = member.replica_id().shard_index(),
                        .replica_index = member.replica_id().replica_index()},
            .endpoint{.ip = member.endpoint().ip(),
                      .port = member.endpoint().port()},
        };
        param.members.push_back(std::move(one));
    }
    param.local_endpoint = {.ip = CONF_GET_STR("ip"),
                            .port = CONF_GET_INT("port")};
    param.data_dir =
        adviskv::common::path_from_config("data_dir")
            .string();

    Status status = replica_manager_->add_replica(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::DeleteReplica(
    grpc::ServerContext* context, const rpc::DeleteReplicaRequest* request,
    rpc::DeleteReplicaResponse* response) {
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::GetReplicaInfo(
    grpc::ServerContext* context, const rpc::GetReplicaInfoRequest* request,
    rpc::GetReplicaInfoResponse* response) {
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::RequestVote(
    grpc::ServerContext* context, const rpc::RequestVoteRequest* request,
    rpc::RequestVoteResponse* response) {
    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }
    const ReplicaID replica_id{
        .table_id = request->to().table_id(),
        .shard_index = request->to().shard_index(),
        .replica_index = request->to().replica_index(),
    };

    Replica* replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "target replica not found"});
        return grpc::Status::OK;
    }
    RequestVoteParam param{
        .to_replica_id = replica_id,
        .from_replica_id =
            {
                .table_id = request->from().table_id(),
                .shard_index = request->from().shard_index(),
                .replica_index = request->from().replica_index(),
            },
        .term = request->term(),
        .last_log_term = request->last_log_term(),
        .last_log_index = request->last_log_index(),
    };

    RequestVoteResult result;
    Status status = replica->handle_request_vote(param, result);

    fill_base_rsp(response, status);
    if (status.ok()) {
        response->set_term(result.term);
        response->set_vote_granted(result.vote_granted);
    }
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::AppendEntries(
    grpc::ServerContext* context, const rpc::AppendEntriesRequest* request,
    rpc::AppendEntriesResponse* response) {
    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }
    const ReplicaID replica_id{
        .table_id = request->to().table_id(),
        .shard_index = request->to().shard_index(),
        .replica_index = request->to().replica_index(),
    };

    Replica* replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "target replica not found"});
        return grpc::Status::OK;
    }

    AppendEntriesParam param{
        .from_replica_id =
            {
                .table_id = request->from().table_id(),
                .shard_index = request->from().shard_index(),
                .replica_index = request->from().replica_index(),
            },
        .to_replica_id = replica_id,
        .term = request->term(),
        .prev_log_term = request->prev_log_term(),
        .prev_log_index = request->prev_log_index(),
        .leader_commit = request->leader_commit(),
    };
    for (const rpc::LogEntry& one : request->entries()) {
        LogEntry entry{
            .term = one.term(),
            .index = one.index(),
            .op_type = (WriteOpType)one.op_type(),
            .key = one.key(),
            .value = one.value(),
        };
        param.entries.push_back(std::move(entry));
    }

    AppendEntriesResult result;
    Status status = replica->handle_append_entries(param, result);
    if (status.fail()) {
        return grpc::Status::OK;
    }
    fill_base_rsp(response, status);
    response->set_success(result.success);
    response->set_term(result.term);

    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::InstallSnapshot(
    grpc::ServerContext* context, const rpc::InstallSnapshotRequest* request,
    rpc::InstallSnapshotResponse* response) {
    if (!replica_manager_) {
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    ReplicaID replica_id{
        .table_id = request->to().table_id(),
        .shard_index = request->to().shard_index(),
        .replica_index = request->to().replica_index(),
    };

    Replica* replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "target replica not found"});
        return grpc::Status::OK;
    }

    InstallSnapshotParam param{
        .from_replica_id =
            {
                .table_id = request->from().table_id(),
                .shard_index = request->from().shard_index(),
                .replica_index = request->from().replica_index(),
            },
        .to_replica_id = replica_id,
        .term = request->term(),
        .snapshot_index = request->apply_index(),
        .snapshot_term = request->apply_term(),
        .offset = request->offset(),
        .data = request->data(),
        .done = request->done(),
    };

    Status status = replica->handle_install_snapshot(param);

    fill_base_rsp(response, status);
    if (status.ok()) {
        response->set_term(request->term());
    }
    return grpc::Status::OK;
}

}  // namespace adviskv::storage
