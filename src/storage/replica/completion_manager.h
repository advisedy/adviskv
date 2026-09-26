#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "common/model/type.h"
#include "common/status.h"

namespace adviskv::storage {

// 对于Loop里面遇到的sync_submit，用Ticket去管理。
class CompletionManager {
public:
    using Ticket = uint64_t;

    Ticket create();
    Status wait(Ticket ticket);
    Status wait_for(Ticket ticket, Milliseconds timeout, const Status& timeout_status);
    bool finish(Ticket ticket, const Status& status);
    void fail_all(const Status& status);

private:
    struct Pending {
        std::condition_variable cv;
        Status status{Status::OK()};
        bool done{false};
    };

    std::mutex mutex_;
    Ticket next_ticket_{1};
    std::unordered_map<Ticket, Pending> pending_;
};

}  // namespace adviskv::storage
