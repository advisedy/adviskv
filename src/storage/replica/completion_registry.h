#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include "common/completion.h"
#include "common/define.h"
#include "common/status.h"

namespace adviskv::storage {

class CompletionRegistry {
public:
    CompletionRegistry() = default;
    DISALLOW_COPY_AND_ASSIGN(CompletionRegistry)

    bool add(const std::shared_ptr<CompletionBase>& completion) {
        std::lock_guard lock(mutex_);
        if (closed_) {
            return false;
        }
        pending_.insert(completion);
        return true;
    }

    void remove(const std::shared_ptr<CompletionBase>& completion) {
        std::lock_guard lock(mutex_);
        pending_.erase(completion);
    }

    // close，并且把里面没有处理完的 completon 都设置成 status
    void close(const Status& status) {
        std::unordered_set<std::shared_ptr<CompletionBase>> to_fail;
        {
            std::lock_guard lock(mutex_);
            if (closed_) {
                return;
            }
            closed_ = true;
            to_fail.swap(pending_);
        }
        for (auto& c : to_fail) {
            c->fail(status);
        }
    }

    bool closed() const {
        std::lock_guard lock(mutex_);
        return closed_;
    }

    // 测试和断言用
    size_t pending_count() const {
        std::lock_guard lock(mutex_);
        return pending_.size();
    }

private:
    mutable std::mutex mutex_;
    bool closed_{false};
    std::unordered_set<std::shared_ptr<CompletionBase>> pending_;
};

}  // namespace adviskv::storage
