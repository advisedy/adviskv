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
    explicit RaftSender(int32 timeout_ms = 1000);

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
    static std::string target_of(const PeerMember& member);
    rpc::StorageService::StubInterface* stub_for(
        const PeerMember& member) const;

    mutable std::mutex mutex_;
    mutable std::unordered_map<
        std::string, std::unique_ptr<rpc::StorageService::StubInterface>>
        stub_pool_;

    struct InFlightSnapshot {
        ReplicaID target;
        LogIndex snapshot_index;
        Term snapshot_term;
    };
    mutable std::unordered_map<ReplicaID, InFlightSnapshot, ReplicaIDHash>
        in_flight_snapshots_;

    mutable std::mutex in_flight_mutex_;
    int32 timeout_ms_{1000};
};

}  // namespace adviskv::storage