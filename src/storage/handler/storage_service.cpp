#include "storage/handler/storage_service.h"

#include <fmt/format.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <string_view>

#include "common/confmgr.h"
#include "common/defer.h"
#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/path_util.h"
#include "common/proto/replica_id_proto.h"
#include "common/proto/raft_role_proto.h"
#include "common/proto/storage_replica_status_proto.h"
#include "common/status.h"
#include "common/type.h"
#include "storage.pb.h"
#include "storage/model/param.h"
#include "storage/proto/storage_model_proto.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {
namespace {

void record_storage_write_handler_result(std::string_view op_name,
                                         const Status& status) {
    const std::string prefix = fmt::format("storage_{}_handler", op_name);
    if (status.ok()) {
        ADVISKV_METRICS_COUNTER(prefix + "_success");
        return;
    }
    ADVISKV_METRICS_COUNTER(prefix + "_failure");
    switch (status.code()) {
        case StatusCode::NOT_LEADER:
            ADVISKV_METRICS_COUNTER(prefix + "_not_leader");
            break;
        case StatusCode::NOT_YET_COMMIT:
            ADVISKV_METRICS_COUNTER(prefix + "_not_yet_commit");
            break;
        case StatusCode::REPLICA_NOT_FOUND:
            ADVISKV_METRICS_COUNTER(prefix + "_replica_not_found");
            break;
        case StatusCode::REPLICA_MANAGER_NOT_FOUND:
            ADVISKV_METRICS_COUNTER(prefix + "_replica_manager_not_found");
            break;
        default:
            ADVISKV_METRICS_COUNTER(prefix + "_other_error");
            break;
    }
}

}  // namespace

grpc::Status StorageServiceImpl::Put(grpc::ServerContext* context,
                                     const rpc::PutRequest* request,
                                     rpc::PutResponse* response) {
    ADVISKV_METRICS_TIMER("storage_put_handler");
    ADVISKV_METRICS_COUNTER("storage_put_handler_request");

    UNUSED(context);
    Status status = Status::OK();
    auto record_result = Defer(
        [&status]() { record_storage_write_handler_result("put", status); });

    if (!replica_manager_) {
        status = Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                        "replica manager not found"};
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, status);
        return grpc::Status::OK;
    }
    ReplicaID replica_id = decode_pb_replica_id(request->replica_id());
    ReplicaPtr&& replica = replica_manager_->get_replica_by_id(replica_id);

    if (!replica) {
        status = Status{StatusCode::REPLICA_NOT_FOUND, "replica not found"};
        LOG_WARN("replica not found, replica_id = {}", replica_id.to_string());
        fill_base_rsp(response, status);
        return grpc::Status::OK;
    }

    // replica内部执行put操作
    PutParam param{request->key(), request->value()};
    status = replica->put(param);

    if (!status.ok()) {
        LOG_WARN(
            "replica put failed, table_id = {}, shard_id = {}, key = {}, value "
            "= {}, msg = {}",
            replica_id.table_id, replica_id.shard_index, request->key(),
            request->value(), status.msg());
    }

    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::Get(grpc::ServerContext* context,
                                     const rpc::GetRequest* request,
                                     rpc::GetResponse* response) {
    UNUSED(context);

    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }
    ReplicaID replica_id = decode_pb_replica_id(request->replica_id());
    ReplicaPtr&& replica = replica_manager_->get_replica_by_id(replica_id);

    if (!replica) {
        LOG_WARN("replica not found, replica_id = {}", replica_id.to_string());
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "replica not found"});
        return grpc::Status::OK;
    }

    GetParam param{request->key()};
    Value value;

    Status status = replica->get(param, value);

    if (!status.ok()) {
        LOG_WARN(
            "replica get failed, table_id = {}, shard_id = {}, key = {}, msg = "
            "{}",
            replica_id.table_id, replica_id.shard_index, request->key(),
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
    ADVISKV_METRICS_TIMER("storage_delete_handler");
    ADVISKV_METRICS_COUNTER("storage_delete_handler_request");

    UNUSED(context);
    Status status = Status::OK();
    auto record_result = Defer(
        [&status]() { record_storage_write_handler_result("delete", status); });

    if (!replica_manager_) {
        status = Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                        "replica manager not found"};
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, status);
        return grpc::Status::OK;
    }

    ReplicaID replica_id = decode_pb_replica_id(request->replica_id());
    ReplicaPtr&& replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        status = Status{StatusCode::REPLICA_NOT_FOUND, "replica not found"};
        LOG_WARN("replica not found, replica_id = {}", replica_id.to_string());
        fill_base_rsp(response, status);
        return grpc::Status::OK;
    }

    status = replica->del(DelParam{request->key()});

    if (!status.ok()) {
        LOG_WARN(
            "replica delete failed, table_id = {}, shard_id = {}, key = {}, "
            "msg = {}",
            replica_id.table_id, replica_id.shard_index, request->key(),
            status.msg());
    }

    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::CreateReplica(
    grpc::ServerContext* context, const rpc::CreateReplicaRequest* request,
    rpc::CreateReplicaResponse* response) {
    UNUSED(context);

    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    ReplicaInitParam param;
    param.replica_id = decode_pb_replica_id(request->replica_id());
    param.engine_type = static_cast<EngineType>(request->engine_type());
    param.members.clear();
    for (const auto& member : request->initial_members()) {
        param.members.push_back(decode_pb_peer_member(member));
    }
    param.local_endpoint = Endpoint{CONF_GET_STR("ip"), CONF_GET_INT("port")};

    Status status = replica_manager_->add_replica(param);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::DeleteReplica(
    grpc::ServerContext* context, const rpc::DeleteReplicaRequest* request,
    rpc::DeleteReplicaResponse* response) {
    UNUSED(context);
    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    ReplicaID replica_id = decode_pb_replica_id(request->replica_id());
    Status status = replica_manager_->delete_replica(replica_id);
    fill_base_rsp(response, status);
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::GetReplicaInfo(
    grpc::ServerContext* context, const rpc::GetReplicaInfoRequest* request,
    rpc::GetReplicaInfoResponse* response) {
    UNUSED(context);

    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    ReplicaID replica_id = decode_pb_replica_id(request->replica_id());
    ReplicaPtr replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        response->set_exists(false);
        fill_base_rsp(response, Status::OK());
        return grpc::Status::OK;
    }

    response->set_exists(true);
    auto* info = response->mutable_replica();
    info->set_table_id(replica->get_replica_id().table_id);
    info->set_shard_id(replica->get_replica_id().shard_index);
    info->set_replica_seq(replica->get_replica_id().replica_seq);
    info->set_role(to_pb_raft_role(replica->get_role()));
    info->set_status(to_pb_storage_replica_status(replica->get_status()));
    info->set_term(replica->current_term());
    auto* endpoint = info->mutable_endpoint();
    endpoint->set_ip(CONF_GET_STR("ip"));
    endpoint->set_port(CONF_GET_INT("port"));
    fill_base_rsp(response, Status::OK());
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::RequestVote(
    grpc::ServerContext* context, const rpc::RequestVoteRequest* request,
    rpc::RequestVoteResponse* response) {
    UNUSED(context);
    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }
    ReplicaID replica_id{request->to().table_id(), request->to().shard_index(),
                         request->to().replica_seq()};

    ReplicaPtr&& replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "target replica not found"});
        return grpc::Status::OK;
    }
    RequestVoteParam param;
    param.from_replica_id =
        ReplicaID{request->from().table_id(), request->from().shard_index(),
                  request->from().replica_seq()};
    param.to_replica_id = replica_id;
    param.term = request->term();
    param.last_log_index = request->last_log_index();
    param.last_log_term = request->last_log_term();

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
    ADVISKV_METRICS_TIMER("storage_raft_append_entries_handler");
    ADVISKV_METRICS_COUNTER("storage_raft_append_entries_handler_request");

    UNUSED(context);

    if (!replica_manager_) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_append_entries_handler_replica_manager_not_found");
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }
    ReplicaID replica_id{request->to().table_id(), request->to().shard_index(),
                         request->to().replica_seq()};

    ReplicaPtr&& replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_append_entries_handler_replica_not_found");
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "target replica not found"});
        return grpc::Status::OK;
    }

    AppendEntriesParam param;
    param.from_replica_id =
        ReplicaID{request->from().table_id(), request->from().shard_index(),
                  request->from().replica_seq()};
    param.to_replica_id = replica_id;
    param.term = request->term();
    param.prev_log_index = request->prev_log_index();
    param.prev_log_term = request->prev_log_term();
    param.leader_commit = request->leader_commit();
    for (const rpc::LogEntry& one : request->entries()) {
        LogEntry entry;
        if (!decode_pb_log_entry(one, entry)) {
            fill_base_rsp(response,
                          Status::INVALID_ARGUMENT("invalid log entry"));
            return grpc::Status::OK;
        }
        param.entries.push_back(std::move(entry));
    }

    AppendEntriesResult result;
    Status status = replica->handle_append_entries(param, result);
    fill_base_rsp(response, status);
    response->set_success(result.success);
    response->set_term(result.term);
    response->set_last_log_index(result.last_log_index);
    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::InstallSnapshot(
    grpc::ServerContext* context, const rpc::InstallSnapshotRequest* request,
    rpc::InstallSnapshotResponse* response) {
    UNUSED(context);

    if (!replica_manager_) {
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    ReplicaID replica_id{request->to().table_id(), request->to().shard_index(),
                         request->to().replica_seq()};

    ReplicaPtr&& replica = replica_manager_->get_replica_by_id(replica_id);
    if (!replica) {
        fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND,
                                       "target replica not found"});
        return grpc::Status::OK;
    }

    InstallSnapshotParam param;
    param.from_replica_id =
        ReplicaID{request->from().table_id(), request->from().shard_index(),
                  request->from().replica_seq()};
    param.to_replica_id = replica_id;
    param.term = request->term();
    param.snapshot_index = request->apply_index();
    param.snapshot_term = request->apply_term();
    param.offset = request->offset();
    param.data = request->data();
    param.done = request->done();

    Status status = replica->handle_install_snapshot(param);

    fill_base_rsp(response, status);
    response->set_term(replica->current_term());
    response->set_snapshot_watermark(0);

    if (status.ok() || status.code() == StatusCode::ALREADY_EXIST) {
        response->set_snapshot_watermark(
            std::max(param.snapshot_index, replica->snapshot_index()));
    } 
    if (status.fail() && status.code() != StatusCode::ALREADY_EXIST) {
        LOG_WARN("replica handle install snapshot failed, status:{}",
                 status.to_string());
    }

    return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::TestGetReplicaState(
    grpc::ServerContext* context,
    const rpc::TestGetReplicaStateRequest* request,
    rpc::TestGetReplicaStateResponse* response) {
    if (!CONF_GET_BOOL("enable_test_api", "false")) {
        fill_base_rsp(response, Status::ERROR("enable_test_api is false"));
        return grpc::Status::OK;
    }

    UNUSED(context);

    if (!replica_manager_) {
        LOG_WARN("replica manager is nullptr");
        fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND,
                                       "replica manager not found"});
        return grpc::Status::OK;
    }

    ShardID shard_id{
        request->table_id(),
        request->shard_id(),
    };
    ReplicaPtr&& replica = replica_manager_->get_replica_by_shard(shard_id);
    if (!replica) {
        response->set_exists(false);
        fill_base_rsp(response, Status::ERROR("replica not found"));
        return grpc::Status::OK;
    }

    Replica::ReplicaStateForTest res;
    Status status = replica->get_replica_state_for_test(res);
    if (status.fail()) {
        fill_base_rsp(response, status);
        return grpc::Status::OK;
    }
    fill_base_rsp(response, status);
    response->set_exists(true);
    response->set_role(to_pb_raft_role(replica->get_role()));
    response->set_status(to_pb_storage_replica_status(replica->get_status()));
    response->set_current_term(res.current_term);
    response->set_commit_index(res.commit_index);
    response->set_last_applied(res.last_applied);
    response->set_snapshot_index(res.snapshot_index);
    response->set_snapshot_term(res.snapshot_term);
    return grpc::Status::OK;
}

}  // namespace adviskv::storage
