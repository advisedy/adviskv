#pragma once

#include <chrono>
#include <memory>

#include "storage/utility/time_scheduler.h"
namespace adviskv::storage {

class Timer {
   public:
    Timer(std::shared_ptr<TimerScheduler> scheduler, TimerTask task);
    void stop();
    void reset(std::chrono::milliseconds sec);
   private:
};

using TimerPtr = std::shared_ptr<Timer>;

}  // namespace adviskv::storage