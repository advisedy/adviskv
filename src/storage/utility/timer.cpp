#include "storage/utility/timer.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <thread>

namespace adviskv::storage {

struct Timer::Impl {
    std::shared_ptr<TimerScheduler> scheduler;
    TimerTask task;
    std::atomic<bool> running{false};
    std::thread thread;
    std::mutex mutex;

    ~Impl() { stop(); }

    void stop() {
        running = false;
        if (thread.joinable()) {
            thread.detach();
        }
    }

    void schedule(std::chrono::milliseconds delay) {
        stop();
        running = true;
        thread = std::thread([this, delay]() {
            std::this_thread::sleep_for(delay);
            if (running) {
                running = false;
                if (task) {
                    task();
                }
            }
        });
    }
};

Timer::Timer(std::shared_ptr<TimerScheduler> scheduler, TimerTask task)
    : impl_(std::make_shared<Impl>()) {
    impl_->scheduler = std::move(scheduler);
    impl_->task = std::move(task);
}

Timer::~Timer() = default;

void Timer::reset(std::chrono::milliseconds delay) {
    if (impl_) {
        impl_->schedule(delay);
    }
}

void Timer::reset_random(std::chrono::milliseconds down,
                         std::chrono::milliseconds up) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(down.count(), up.count());
    reset(std::chrono::milliseconds(dist(gen)));
}

void Timer::stop() {
    if (impl_) {
        impl_->stop();
    }
}

}  // namespace adviskv::storage
