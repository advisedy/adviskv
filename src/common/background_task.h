#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "common/define.h"
#include "common/status.h"

namespace adviskv {

/*
主要就是继承实现run函数。
使用就是prepare + start，或者手动notify唤醒
*/
class BackgroundTask {
   public:
    BackgroundTask() = default;

    virtual ~BackgroundTask() { stop(); }

   public:
    Status prepare(); // 如果setup是空的，可以不调用这个
    void start(Milliseconds interval);
    void notify();
    void stop();

   protected:
    virtual Status setup() { return Status::OK(); };
    virtual void run() = 0;
    virtual void teardown() {};

   private:
    void loop(Milliseconds interval);

   private:
    std::atomic<bool> running_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::unique_ptr<std::thread> thread_;
};

}  // namespace adviskv