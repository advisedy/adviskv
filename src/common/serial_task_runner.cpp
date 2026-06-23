#include "common/serial_task_runner.h"

#include <utility>

namespace adviskv {

thread_local SerialTaskRunner* current_runner_ = nullptr;

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

bool SerialTaskRunner::runs_in_current_thread() const {
    return current_runner_ == this;
}

void SerialTaskRunner::loop() {
    current_runner_ = this;
    while (true) {
        Task task;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this] { return !running_ || !queue_.empty(); });
            if (!running_ && queue_.empty()) {
                break;
            }
            task = std::move(queue_.front());
            queue_.pop_front();
        }
        task();
    }
    current_runner_ = nullptr;
}

}  // namespace adviskv