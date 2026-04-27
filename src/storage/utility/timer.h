#pragma once

#include <chrono>
#include <memory>

#include "storage/utility/time_scheduler.h"
namespace adviskv::storage {

class Timer {
   public:
    Timer(std::shared_ptr<TimerScheduler> scheduler, TimerTask task);
    void reset(std::chrono::milliseconds sec);
    void reset_random(std::chrono::milliseconds down, std::chrono::milliseconds up);
    void stop();
   private:
};

using TimerPtr = std::shared_ptr<Timer>;

}  // namespace adviskv::storage