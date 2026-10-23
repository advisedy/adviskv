#pragma once

#include <functional>
#include <vector>

#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"
#include "common/thread_pool.h"
#include "storage/model/model.h"
#include "storage/raft/raft_sender.h"
#include "storage/replica/replica_work.h"

namespace adviskv::storage {

class PersistEngine;

class ReplicaMessageDispatcher {
public:
    using EventCallback = std::function<void(Event)>;

    ReplicaMessageDispatcher(int32 raft_rpc_timeout_ms, PersistEngine& persist, EventCallback event_callback);
    ~ReplicaMessageDispatcher();

    DISALLOW_COPY_AND_ASSIGN(ReplicaMessageDispatcher)

    void start();
    void stop();

    Status async_send(std::vector<RaftMessage> messages);

private:
    Status async_send_one(const RaftMessage& msg);
    void send_task(RaftMessage msg);
    void callback(Event event);

    RaftSender sender_;
    PersistEngine& persist_;
    EventCallback event_callback_;
    ThreadPool rpc_pool_;
};

}  // namespace adviskv::storage
