#pragma once

#include <grpcpp/grpcpp.h>

#include "common/define.h"
#include "common/status.h"
#include "storage.grpc.pb.h"
#include "storage.pb.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"

namespace adviskv::storage {

// using RequestVoteCallback =
//     std::function<void(const Status&, const RequestVoteResult&)>;

class RaftSender {
   public:
    // TODO 以后要改
    Status send_request_vote(const PeerMember& member,
                             const RequestVoteParam& param,
                             RequestVoteResult& result) const {
        auto channel = grpc::CreateChannel(
            member.endpoint.ip + ":" + std::to_string(member.endpoint.port),
            grpc::InsecureChannelCredentials());
        auto stub = rpc::StorageService::NewStub(channel);

        rpc::RequestVoteRequest request;
        request.mutable_from()->set_table_id(param.from_replica_id.table_id);
        request.mutable_from()->set_shard_index(
            param.from_replica_id.shard_index);
        request.mutable_from()->set_replica_index(
            param.from_replica_id.replica_index);

        request.mutable_to()->set_table_id(param.to_replica_id.table_id);
        request.mutable_to()->set_shard_index(param.to_replica_id.shard_index);
        request.mutable_to()->set_replica_index(
            param.to_replica_id.replica_index);

        request.set_term(param.term);
        request.set_last_log_index(param.last_log_index);
        request.set_last_log_term(param.last_log_term);

        rpc::RequestVoteResponse response;
        grpc::ClientContext context;
        grpc::Status grpc_status =
            stub->RequestVote(&context, request, &response);
        RETURN_IF_INVALID_CONDITION(grpc_status.ok(),
                                    grpc_status.error_message())

        result.term = response.term();
        result.vote_granted = response.vote_granted();
        return Status::OK();
    }

    Status send_append_entries(const PeerMember& member,
                               const AppendEntriesParam& param,
                               AppendEntriesResult& result) const {
        // TODO 对于以后来说，应该不能每一次都重新连接，
        // 可能需要提前练好搞个连接池？ 待定吧先.
        auto channel = grpc::CreateChannel(
            member.endpoint.ip + ":" + std::to_string(member.endpoint.port),
            grpc::InsecureChannelCredentials());
        auto stub = rpc::StorageService::NewStub(channel);

        rpc::AppendEntriesRequest request;
        request.mutable_from()->set_table_id(param.from_replica_id.table_id);
        request.mutable_from()->set_shard_index(
            param.from_replica_id.shard_index);
        request.mutable_from()->set_replica_index(
            param.from_replica_id.replica_index);

        request.mutable_to()->set_table_id(param.to_replica_id.table_id);
        request.mutable_to()->set_shard_index(param.to_replica_id.shard_index);
        request.mutable_to()->set_replica_index(
            param.to_replica_id.replica_index);

        request.set_term(param.term);
        request.set_prev_log_index(param.prev_log_index);
        request.set_prev_log_term(param.prev_log_term);
        request.set_leader_commit(param.leader_commit);

        for (const LogEntry& entry : param.entries) {
            auto one = request.add_entries();
            one->set_index(entry.index);
            one->set_term(entry.term);
            one->set_op_type((int32_t)entry.op_type);
            one->set_key(entry.key);
            one->set_value(entry.value);
        }

        rpc::AppendEntriesResponse response;
        grpc::ClientContext context;
        grpc::Status grpc_status =
            stub->AppendEntries(&context, request, &response);
        RETURN_IF_INVALID_CONDITION(grpc_status.ok(),
                                    grpc_status.error_message())

        result.term = response.term();
        result.success = response.success();
        return Status::OK();
    }

    Status send_install_snapshot(const PeerMember& member,
                                 const InstallSnapshotParam& param,
                                 const PersistEngine& persist,
                                 InstallSnapshotResult& result) const {
        auto channel = grpc::CreateChannel(
            member.endpoint.ip + ":" + std::to_string(member.endpoint.port),
            grpc::InsecureChannelCredentials());
        auto stub = rpc::StorageService::NewStub(channel);

        constexpr size_t kChunkSize = 1 << 20;
        uint64 offset = 0;
        result.term = param.term;
        result.success = false;
        while (true) {
            std::string data;
            bool eof = false;
            RETURN_IF_INVALID_STATUS(
                persist.read_snapshot_chunk(offset, kChunkSize, data, eof))

            rpc::InstallSnapshotRequest request;
            request.mutable_from()->set_table_id(param.from_replica_id.table_id);
            request.mutable_from()->set_shard_index(
                param.from_replica_id.shard_index);
            request.mutable_from()->set_replica_index(
                param.from_replica_id.replica_index);

            request.mutable_to()->set_table_id(param.to_replica_id.table_id);
            request.mutable_to()->set_shard_index(
                param.to_replica_id.shard_index);
            request.mutable_to()->set_replica_index(
                param.to_replica_id.replica_index);

            request.set_term(param.term);
            request.set_apply_index(param.snapshot_index);
            request.set_apply_term(param.snapshot_term);
            request.set_offset(offset);
            request.set_data(data);
            request.set_done(eof);

            rpc::InstallSnapshotResponse response;
            grpc::ClientContext context;
            grpc::Status grpc_status =
                stub->InstallSnapshot(&context, request, &response);
            RETURN_IF_INVALID_CONDITION(grpc_status.ok(),
                                        grpc_status.error_message())

            result.term = response.term();
            if (response.base_rsp().code() !=
                static_cast<int32_t>(StatusCode::OK)) {
                result.success = false;
                return Status::OK();
            }

            if (eof) {
                result.success = true;
                break;
            }
            offset += data.size();
        }
        return Status::OK();
    }
};

}  // namespace adviskv::storage