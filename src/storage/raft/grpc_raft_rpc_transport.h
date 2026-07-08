#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "storage.grpc.pb.h"
#include "storage/raft/iraft_rpc_transport.h"

namespace adviskv::storage {

class GrpcRaftRpcTransport final : public IRaftRpcTransport {
public:
    Status request_vote(const PeerMember& target, const RequestVoteParam& param, int32_t timeout_ms,
                        RequestVoteResult& result) const override;

    Status append_entries(const PeerMember& target, const AppendEntriesParam& param, int32_t timeout_ms,
                          AppendEntriesResult& result) const override;

    Status install_snapshot_chunk(const PeerMember& target, const InstallSnapshotParam& param, int32_t timeout_ms,
                                  InstallSnapshotResult& result) const override;

private:
    static std::string target_of(const PeerMember& member);
    rpc::StorageService::StubInterface* stub_for(const PeerMember& member) const;

    mutable std::mutex mutex_;
    mutable std::unordered_map<std::string, std::unique_ptr<rpc::StorageService::StubInterface>> stub_pool_;
};

}  // namespace adviskv::storage