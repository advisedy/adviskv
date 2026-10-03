#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"

namespace adviskv {

/*
主要就是继承实现run函数。
使用就是setup + start，或者手动notify唤醒
*/
class BackgroundTask {
public:
    BackgroundTask() = default;

    virtual ~BackgroundTask() { stop(); }

public:
    virtual Status setup() { return Status::OK(); };
    void start(Milliseconds interval, bool run_immediately = false);
    void notify();
    void stop();

protected:
    virtual void run() = 0;
    virtual void teardown() {};

private:
    void loop(Milliseconds interval, bool run_immediately);

private:
    std::atomic<bool> running_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::unique_ptr<std::thread> thread_;
};

}  // namespace adviskv