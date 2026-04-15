#include "sdm/background/background_task.h"

#include <memory>
#include <mutex>
#include <thread>

namespace adviskv::sdm {

void BackgroundTask::prepare() {
  setup();
  run();
}

void BackgroundTask::start(std::chrono::milliseconds interval) {
  if (interval.count() <= 0) {
    return;
  }
  if (running_.exchange(true)) { // 防止搞了多个background task
    return;
  }
  thread_ =
      std::make_unique<std::thread>(&BackgroundTask::loop, this, interval);
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

void BackgroundTask::loop(std::chrono::milliseconds interval) {

  while (running_) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, interval);
      if (!running_) {
        break;
      }
    }
    run();
  }
}

} // namespace adviskv::sdm
