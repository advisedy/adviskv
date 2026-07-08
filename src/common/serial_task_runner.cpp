#include "common/serial_task_runner.h"

#include <utility>

namespace adviskv {

SerialTaskRunner::~SerialTaskRunner() { stop(); }

void SerialTaskRunner::start() {
    std::lock_guard lock(mutex_);
    if (running_) {
        return;
    }
    running_ = true;
    thread_ = std::thread(&SerialTaskRunner::loop, this);
}

void SerialTaskRunner::stop() {
    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return;
        }
        running_ = false;
    }
    cv_.notify_all();

    if (thread_.joinable()) {
        thread_.join();
    }
}

bool SerialTaskRunner::submit(Task&& task) {
    if (!task) {
        return false;
    }

    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return false;
        }
        queue_.push_back(std::move(task));
    }
    cv_.notify_one();
    return true;
}

bool SerialTaskRunner::is_running() const {
    std::lock_guard lock(mutex_);
    return running_;
}

void SerialTaskRunner::loop() {
    while (true) {
        Task task;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this]() { return !running_ || !queue_.empty(); });
            if (!running_ && queue_.empty()) {
                break;
            }
            task = std::move(queue_.front());
            queue_.pop_front();
        }
        task();
    }
}

}  // namespace adviskv