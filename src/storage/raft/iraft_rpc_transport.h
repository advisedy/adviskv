#pragma once

#include "common/status.h"
#include "common/model/type.h"
#include "storage/model/model.h"

namespace adviskv::storage {

class IRaftRpcTransport {
   public:
    virtual ~IRaftRpcTransport() = default;

    virtual Status request_vote(const PeerMember& target,
                                const RequestVoteParam& param,
                                int32_t timeout_ms,
                                RequestVoteResult& result) const = 0;

    virtual Status append_entries(const PeerMember& target,
                                  const AppendEntriesParam& param,
                                  int32_t timeout_ms,
                                  AppendEntriesResult& result) const = 0;

    virtual Status install_snapshot_chunk(
        const PeerMember& target, const InstallSnapshotParam& param,
        int32_t timeout_ms, InstallSnapshotResult& result) const = 0;
};

}  // namespace adviskv::storage
