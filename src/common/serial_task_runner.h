#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

#include "common/define.h"

namespace adviskv {

class SerialTaskRunner {
   public:
    using Task = std::function<void()>;

    SerialTaskRunner() = default;
    ~SerialTaskRunner();

    DISALLOW_COPY_AND_ASSIGN(SerialTaskRunner)

    void start();
    void stop();
    bool submit(Task&& task);
    bool is_running() const;
    bool runs_in_current_thread() const;

   private:
    void loop();

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Task> queue_;
    bool running_{false};
    std::thread thread_;

};

}  // namespace adviskv