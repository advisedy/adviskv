#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "common/define.h"

namespace adviskv {

class ThreadPool {
public:
    ThreadPool() = default;
    explicit ThreadPool(size_t worker_count);

    ~ThreadPool();

    DISALLOW_COPY_AND_ASSIGN(ThreadPool)

public:
    void start(size_t worker_count);
    void stop();

    void submit(std::function<void()> task);

    bool started() const;

private:
    void worker_loop();

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    bool stopping_{false};
    bool started_{false};
};

}  // namespace adviskv