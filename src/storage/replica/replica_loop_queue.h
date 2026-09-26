#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <variant>
#include <vector>

#include "common/define.h"
#include "storage/replica/proposal_tracker.h"

namespace adviskv::storage {

// 队列里面主要放了两种Task，普通Task取出来直接执行，WritesTask取出来会交给Tracker搞后续了。
class ReplicaLoopQueue {
public:
    using Task = std::function<void()>;

    explicit ReplicaLoopQueue(ProposalTracker& tracker);
    ~ReplicaLoopQueue();

    DISALLOW_COPY_AND_ASSIGN(ReplicaLoopQueue)

    void start(size_t max_batch_size);
    void stop();
    bool post_write(ProposeParam param, CompletionManager::Ticket ticket);
    bool post_task(Task task);

private:
    struct WritesTask {
        std::vector<ProposalItem> items;
    };

    void run();

    ProposalTracker& tracker_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::variant<WritesTask, Task>> tasks_;
    std::thread worker_;
    size_t max_batch_size_{0};
    bool running_{false};
};

}  // namespace adviskv::storage
