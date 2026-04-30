#pragma once

#include <chrono>
#include <memory>

#include "storage/utility/time_scheduler.h"
namespace adviskv::storage {

class Timer {
   public:
    Timer(std::shared_ptr<TimerScheduler> scheduler, TimerTask task);
    ~Timer();

    void reset(std::chrono::milliseconds sec);
    void reset_random(std::chrono::milliseconds down, std::chrono::milliseconds up);
    void stop();

   private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

using TimerPtr = std::shared_ptr<Timer>;

}  // namespace adviskv::storage