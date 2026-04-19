#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace adviskv {

/*
主要就是继承实现run函数。
*/
class BackgroundTask {
   public:
    BackgroundTask() = default;

    virtual ~BackgroundTask() { stop(); }

   public:
    void prepare();
    void start(std::chrono::milliseconds interval);
    void notify();
    void stop();

   protected:
    virtual void setup() {};
    virtual void run() = 0;
    virtual void teardown() {};

   private:
    void loop(std::chrono::milliseconds interval);

   private:
    std::atomic<bool> running_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::unique_ptr<std::thread> thread_;
};

}  // namespace adviskv::sdm
