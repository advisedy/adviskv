#pragma once

#include <grpcpp/grpcpp.h>

#include "common/define.h"
#include "common/status.h"
#include "storage.grpc.pb.h"
#include "storage.pb.h"
#include "storage/model/param.h"

namespace adviskv::storage {

// using RequestVoteCallback =
//     std::function<void(const Status&, const RequestVoteResult&)>;

class RaftSender {
   public:
    // TODO 这里看看以后要不要改改
    Status send_request_vote(const PeerMember& member,
                             const RequestVoteParam& param,
                             RequestVoteResult& result)const {
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

    Status send_append_entries(const PeerMember& member, const AppendEntriesParam&param, AppendEntriesResult& result)const{

        //TODO

        return Status::OK();
    }
};

}  // namespace adviskv::storage