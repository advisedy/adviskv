#include "common/background_task.h"

#include <memory>
#include <mutex>
#include <thread>

namespace adviskv {

void BackgroundTask::start(Milliseconds interval, bool run_immediately) {
    if (interval.count() <= 0) {
        return;
    }
    if (running_.exchange(true)) {  // 防止搞了多个background task
        return;
    }
    thread_ = std::make_unique<std::thread>(&BackgroundTask::loop, this, interval, run_immediately);
}

void BackgroundTask::notify() { cv_.notify_one(); }

void BackgroundTask::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    cv_.notify_one();
    if (thread_ && thread_->joinable()) {
        thread_->join();
        thread_.reset();
    }
    teardown();
}

void BackgroundTask::loop(Milliseconds interval, bool run_immediately) {
    bool first_run = run_immediately;
    while (running_) {
        if (!first_run) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, interval);
            if (!running_) {
                break;
            }
        }
        first_run = false;
        run();
    }
}

}  // namespace adviskv
