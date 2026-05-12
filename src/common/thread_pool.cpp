#include "common/thread_pool.h"

namespace adviskv {

ThreadPool::ThreadPool(size_t worker_count) {
    start(worker_count);
}

ThreadPool::~ThreadPool() {
    stop();
}

void ThreadPool::start(size_t worker_count) {
    if (worker_count == 0) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) {
        return;
    }

    stopping_ = false;
    started_ = true;
    workers_.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&ThreadPool::worker_loop, this);
    }
}

void ThreadPool::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) {
            return;
        }
        stopping_ = true;
    }
    cv_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();

    std::lock_guard<std::mutex> lock(mutex_);
    while (!tasks_.empty()) {
        tasks_.pop();
    }
    started_ = false;
    stopping_ = false;
}

void ThreadPool::submit(std::function<void()> task) {
    if (!task) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_ || stopping_) {
            return;
        }
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
}

bool ThreadPool::started() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return started_;
}

void ThreadPool::worker_loop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
    }
}

}  // namespace adviskv
