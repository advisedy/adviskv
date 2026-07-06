#pragma once

#include <memory>

#include "common/status.h"
#include "storage/model/model.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/iraft_rpc_transport.h"

namespace adviskv::storage {

class RaftSender {
   public:
    explicit RaftSender(int32 timeout_ms = 1000);
    RaftSender(std::unique_ptr<IRaftRpcTransport> transport, int32 timeout_ms);

    void set_timeout_ms(int32 timeout_ms);

    Status send_request_vote(const PeerMember& member,
                             const RequestVoteParam& param,
                             RequestVoteResult& result) const;

    Status send_append_entries(const PeerMember& member,
                               const AppendEntriesParam& param,
                               AppendEntriesResult& result) const;

    Status send_install_snapshot(const PeerMember& member,
                                 const InstallSnapshotParam& param,
                                 const PersistEngine& persist,
                                 InstallSnapshotResult& result) const;

   private:
    std::unique_ptr<IRaftRpcTransport> transport_;
    int32 timeout_ms_{1000};
};

}  // namespace adviskv::storage
