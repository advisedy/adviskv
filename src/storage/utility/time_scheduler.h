#pragma once

#include <functional>

namespace adviskv::storage{

using TimerTask = std::function<void()>;

class TimerScheduler{
};

}