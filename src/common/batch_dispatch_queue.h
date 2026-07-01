#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "common/define.h"

namespace adviskv {

template <typename Item>
class BatchDispatchQueue {
public:
    using DispatchCallback = std::function<void(std::vector<Item>)>;

    BatchDispatchQueue() = default;
    ~BatchDispatchQueue(){IGNORE_RESULT(stop())}

    DISALLOW_COPY_AND_ASSIGN(BatchDispatchQueue)

            void start(std::chrono::steady_clock::duration batch_delay, std::size_t max_batch_size,
                       DispatchCallback on_dispatch) {
        std::lock_guard lock(mutex_);
        state_ = State::NORMAL;
        batch_delay_ = batch_delay;
        max_batch_size_ = max_batch_size;
        on_dispatch_ = std::move(on_dispatch);
        timer_thread_ = std::thread(&BatchDispatchQueue::timer_loop, this);
    }

    std::deque<Item> stop() {
        std::deque<Item> items;
        {
            std::lock_guard lock(mutex_);
            state_ = State::STOPPED;
            items.swap(items_);
        }
        cv_.notify_all();

        if (timer_thread_.joinable()) {
            timer_thread_.join();
        }
        return items;
    }

    // 是否push成功
    bool push(Item item) {
        std::lock_guard lock(mutex_);
        if (state_ == State::STOPPED) {
            return false;
        }
        items_.push_back(std::move(item));

        if (state_ == State::WAITING_TIMER) {
            if (enable_dispatch_unlocked()) {
                dispatch_all_unlocked();
            }
            return true;
        }

        if (state_ == State::NORMAL) {
            schedule_timer_unlocked();
        }
        return true;
    }

private:
    enum class State : int8_t { NORMAL = 0, WAITING_TIMER = 1, STOPPED = 2 };

    bool enable_dispatch_unlocked() const {
        return items_.size() >= max_batch_size_;
    }

    void schedule_timer_unlocked() {
        if (state_ == State::STOPPED) {
            return;
        }
        state_ = State::WAITING_TIMER;
        timer_deadline_ = std::chrono::steady_clock::now() + batch_delay_;
        cv_.notify_one();
    }

    void dispatch_all_unlocked() {
        if (state_ == State::STOPPED || items_.empty()) {
            return;
        }
        std::vector<Item> batch;
        while (!items_.empty()) {
            batch.emplace_back(std::move(items_.front()));
            items_.pop_front();
        }

        state_ = State::NORMAL;
        cv_.notify_one();

        on_dispatch_(std::move(batch));
    }

    void timer_loop() {
        while (true) {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this] { return state_ == State::STOPPED || state_ == State::WAITING_TIMER; });
            if (state_ == State::STOPPED) {
                break;
            }

            cv_.wait_until(lock, timer_deadline_,
                           [this] { return state_ == State::STOPPED || state_ == State::NORMAL; });
            if (state_ == State::STOPPED) {
                break;
            }
            if (state_ == State::NORMAL) {
                continue;
            }
            dispatch_all_unlocked();
        }
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Item> items_;
    DispatchCallback on_dispatch_;
    std::chrono::steady_clock::duration batch_delay_{};
    std::chrono::steady_clock::time_point timer_deadline_{};
    std::size_t max_batch_size_{0};
    State state_{State::STOPPED};
    std::thread timer_thread_;
};

}  // namespace adviskv