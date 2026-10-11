#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace adviskv::testhook {

class PausePointRegistry {
public:
    using Callback = std::function<bool()>;

    static PausePointRegistry& instance() {
        static PausePointRegistry registry;
        return registry;
    }

    void register_point(const std::string& name, Callback callback) {
        std::lock_guard lock(mutex_);
        callbacks_[name] = std::move(callback);
        registered_count_.store(callbacks_.size(), std::memory_order_release);
    }

    void unregister_point(const std::string& name) {
        std::lock_guard lock(mutex_);
        callbacks_.erase(name);
        resumed_.erase(name);
        registered_count_.store(callbacks_.size(), std::memory_order_release);
        resume_cv_.notify_all();
    }

    bool pause(const char* name) {
        if (registered_count_.load(std::memory_order_relaxed) == 0) {
            return false;
        }
        return pause(std::string(name));
    }

    bool pause(const std::string& name) {
        Callback callback;
        bool pre_resumed = false;
        {
            std::unique_lock lock(mutex_);
            auto it = callbacks_.find(name);
            if (it == callbacks_.end()) {
                return false;
            }
            callback = it->second;
            pre_resumed = resumed_.erase(name) > 0;
        }

        if (callback && !callback()) {
            return true;
        }
        if (pre_resumed) {
            return true;
        }

        std::unique_lock lock(mutex_);
        (void)resume_cv_.wait_for(lock, std::chrono::seconds(30),
                                  [this, &name] { return resumed_.find(name) != resumed_.end(); });
        resumed_.erase(name);
        return true;
    }

    void resume(const std::string& name) {
        std::lock_guard lock(mutex_);
        resumed_.insert(name);
        resume_cv_.notify_all();
    }

private:
    std::atomic<size_t> registered_count_{0};
    std::mutex mutex_;
    std::unordered_map<std::string, Callback> callbacks_;
    std::condition_variable resume_cv_;
    std::unordered_set<std::string> resumed_;
};

inline void pause_point(const char* name) { PausePointRegistry::instance().pause(name); }

}  // namespace adviskv::testhook
