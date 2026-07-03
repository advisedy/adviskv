#pragma once

#include <functional>
#include <mutex>
#include <vector>

#include "common/define.h"
#include "common/status.h"
#include "common/thread_pool.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/raft_sender.h"
#include "storage/replica/replica_loop.h"

namespace adviskv::storage {

class PersistEngine;

class ReplicaMessageDispatcher {
public:
    using EventCallback = std::function<void(Event)>;

    // 这里不用Context了，免得循环了，能简单点就简单点吧
    ReplicaMessageDispatcher(int32 raft_rpc_timeout_ms,
                             PersistEngine& persist,
                             std::mutex& persist_snapshot_mutex,
                             EventCallback event_callback);
    ~ReplicaMessageDispatcher();

    DISALLOW_COPY_AND_ASSIGN(ReplicaMessageDispatcher)

    void start();
    void stop();

    Status async_send(std::vector<RaftMessage> messages);

    // 专门给ReadIndexChecker搞得接口
    Status sync_send_append_entries(const PeerMember& target,
                                    const AppendEntriesParam& param,
                                    AppendEntriesResult& result);
    Status sync_send_install_snapshot(const PeerMember& target,
                                      const InstallSnapshotParam& param,
                                      InstallSnapshotResult& result);

private:
    Status async_send_one(const RaftMessage& msg);
    void send_task(RaftMessage msg);
    void callback(Event event);

    RaftSender sender_;
    PersistEngine& persist_;
    std::mutex& persist_snapshot_mutex_;
    EventCallback event_callback_;
    ThreadPool rpc_pool_;
};

}  // namespace adviskv::storage