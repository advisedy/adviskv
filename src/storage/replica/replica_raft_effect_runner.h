#pragma once

#include <functional>
#include <vector>

#include "common/status.h"
#include "common/thread_pool.h"
#include "storage/model/param.h"
#include "storage/raft/raft_sender.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

// 负责把 RaftNode 产出的 effects 真正落到持久化和网络 RPC 上。
class ReplicaRaftEffectRunner {
   public:
    using RaftStepFunc = std::function<Status(RaftEffects&)>;
    struct ResponseHandlers {
        std::function<void(const ReplicaID&, const RequestVoteResult&)>
            request_vote;
        std::function<void(const ReplicaID&, const AppendEntriesParam&,
                           const AppendEntriesResult&)>
            append_entries;
        std::function<void(const ReplicaID&, const AppendEntriesParam&,
                           const Status&)>
            append_entries_failed;
        std::function<void(const ReplicaID&, const InstallSnapshotParam&,
                           const InstallSnapshotResult&)>
            install_snapshot;
        std::function<void(const ReplicaID&, const InstallSnapshotParam&,
                           const Status&)>
            install_snapshot_failed;
    };

    ReplicaRaftEffectRunner(ReplicaContext& context, int32 raft_rpc_timeout_ms);
    ~ReplicaRaftEffectRunner();

    void set_response_handlers(ResponseHandlers handlers);
    void start_rpc_workers(size_t worker_count);
    void stop_rpc_workers();
    Status run_raft_step(RaftStepFunc&& step);
    Status persist_raft_effects(const RaftEffects& effects);
    Status sync_send_append_entries(const PeerMember& member,
                                    const AppendEntriesParam& param,
                                    AppendEntriesResult& result);
    Status sync_send_install_snapshot(const PeerMember& member,
                                      const InstallSnapshotParam& param,
                                      InstallSnapshotResult& result);

   private:
    Status send_raft_messages(std::vector<RaftMessage> messages);
    Status send_raft_message(const RaftMessage& msg);
    void send_raft_message_task(RaftMessage msg);

    ReplicaContext& context_;
    RaftSender raft_sender_;
    ResponseHandlers response_handlers_;
    ThreadPool rpc_pool_;
};

}  // namespace adviskv::storage