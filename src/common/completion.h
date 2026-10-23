#pragma once

#include <cassert>
#include <condition_variable>
#include <mutex>
#include <utility>

#include "common/define.h"
#include "common/optional.h"
#include "common/status.h"
#include "common/types.h"

namespace adviskv {

class CompletionBase {
public:
    virtual ~CompletionBase() = default;
    virtual void fail(const Status& status) = 0;
};

// 内部观测用的状态
enum class CompletionState {
    Pending = 0,
    Completed = 1,
    TimedOut = 2,
};

// 给业务那边看的状态
enum class CompleteStatus {
    Delivered = 0,         // 送到了，等待方会看到这次结果
    AlreadyCompleted = 1,  // 之前已经有 Completed 了
    TimedOut = 2,          // 等待方早就超时走了，这次结果没人要
};

template <typename T>
struct CompletionResult {
    Status status;
    Optional<T> value;

    static CompletionResult ok(T v) { return {Status::OK(), Optional<T>(std::move(v))}; }
    static CompletionResult fail(Status s) { return {std::move(s), std::nullopt}; }

private:
    CompletionResult(Status s, Optional<T> v) : status(std::move(s)), value(std::move(v)) {}
};

template <>
struct CompletionResult<void> {
    Status status;

    static CompletionResult ok() { return CompletionResult(Status::OK()); }
    static CompletionResult fail(Status s) { return CompletionResult(std::move(s)); }

private:
    explicit CompletionResult(Status s) : status(std::move(s)) {}
};

template <typename T>
class Completion : public CompletionBase {
public:
    Completion() = default;
    DISALLOW_COPY_AND_ASSIGN(Completion)

    CompleteStatus complete(CompletionResult<T> result) {
        std::lock_guard lock(mutex_);
        if (state_ == CompletionState::Completed) {
            return CompleteStatus::AlreadyCompleted;
        }
        if (state_ == CompletionState::TimedOut) {
            return CompleteStatus::TimedOut;
        }
        result_.emplace(std::move(result));
        state_ = CompletionState::Completed;
        cv_.notify_one();
        return CompleteStatus::Delivered;
    }

    CompletionResult<T> wait() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return state_ != CompletionState::Pending; });
        return take_result();
    }

    CompletionResult<T> wait_for(Milliseconds timeout, const Status& timeout_status) {
        std::unique_lock lock(mutex_);
        if (!cv_.wait_for(lock, timeout, [this] { return state_ != CompletionState::Pending; })) {
            result_.emplace(CompletionResult<T>::fail(timeout_status));
            state_ = CompletionState::TimedOut;
        }
        return take_result();
    }

    bool needs_result() const {
        std::lock_guard lock(mutex_);
        return state_ == CompletionState::Pending;
    }

    void fail(const Status& status) override{IGNORE_RESULT(complete(CompletionResult<T>::fail(status)))}

    CompletionState state() const {
        std::lock_guard lock(mutex_);
        return state_;
    }

private:
    CompletionResult<T> take_result() {
        assert(result_.has_value());
        CompletionResult<T> result = std::move(*result_);
        result_.reset();
        return result;
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    CompletionState state_{CompletionState::Pending};
    Optional<CompletionResult<T>> result_;
};

}  // namespace adviskv
