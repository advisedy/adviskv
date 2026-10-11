#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <utility>

#include "common/define.h"

namespace adviskv {

template <typename T>
class BlockingQueue {
public:
    BlockingQueue() = default;
    ~BlockingQueue() = default;

    DISALLOW_COPY_AND_ASSIGN(BlockingQueue)
    DISALLOW_MOVE_AND_ASSIGN(BlockingQueue)

    bool push(T item) {
        {
            std::lock_guard lock(mutex_);
            if (closed_) {
                return false;
            }
            items_.push_back(std::move(item));
        }
        cv_.notify_one();
        return true;
    }

    bool pop(T& item) {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this]() { return closed_ || !items_.empty(); });
        if (items_.empty()) {
            return false;
        }
        item = std::move(items_.front());
        items_.pop_front();
        return true;
    }

    bool try_pop(T& item) {
        std::lock_guard lock(mutex_);
        if (closed_ || items_.empty()) {
            return false;
        }
        item = std::move(items_.front());
        items_.pop_front();
        return true;
    }

    void close() {
        std::deque<T> discarded;
        {
            std::lock_guard lock(mutex_);
            closed_ = true;
            discarded.swap(items_);
        }
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<T> items_;
    bool closed_{false};
};

}
