#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

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
    using StubFactory =
        std::function<std::unique_ptr<rpc::StorageService::StubInterface>(
            const PeerMember&)>;

    RaftSender();
    explicit RaftSender(StubFactory stub_factory);

    Status send_request_vote(const PeerMember& member,
                             const RequestVoteParam& param,
                             RequestVoteResult& result) const {
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
            stub_for(member)->RequestVote(&context, request, &response);
        RETURN_IF_INVALID_CONDITION(grpc_status.ok(),
                                    grpc_status.error_message())

        result.term = response.term();
        result.vote_granted = response.vote_granted();
        return Status::OK();
    }

    Status send_append_entries(const PeerMember& member,
                               const AppendEntriesParam& param,
                               AppendEntriesResult& result) const {
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
            stub_for(member)->AppendEntries(&context, request, &response);
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
        constexpr size_t kChunkSize = 1 << 20;
        uint64 offset = 0;
        rpc::StorageService::StubInterface* stub = stub_for(member);
        result.term = param.term;
        result.success = false;
        while (true) {
            std::string data;
            bool eof = false;
            RETURN_IF_INVALID_STATUS(
                persist.read_snapshot_chunk(offset, kChunkSize, data, eof))

            rpc::InstallSnapshotRequest request;
            request.mutable_from()->set_table_id(
                param.from_replica_id.table_id);
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

   private:
    static std::string target_of(const PeerMember& member);
    
    static std::unique_ptr<rpc::StorageService::StubInterface>
    default_stub_factory(const PeerMember& member);

    rpc::StorageService::StubInterface* stub_for(
        const PeerMember& member) const;

    StubFactory stub_factory_;
    mutable std::mutex mutex_;
    mutable std::unordered_map<
        std::string, std::unique_ptr<rpc::StorageService::StubInterface>>
        stub_pool_;
};

}  // namespace adviskv::storage