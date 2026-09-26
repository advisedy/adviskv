#include "storage/replica/replica_loop_queue.h"

#include <utility>

#include "common/log.h"

namespace adviskv::storage {

ReplicaLoopQueue::ReplicaLoopQueue(ProposalTracker& tracker) : tracker_(tracker) {}

ReplicaLoopQueue::~ReplicaLoopQueue() { stop(); }

void ReplicaLoopQueue::start(size_t max_batch_size) {
    std::lock_guard lock(mutex_);
    if (running_) {
        return;
    }
    max_batch_size_ = max_batch_size;
    running_ = true;
    worker_ = std::thread(&ReplicaLoopQueue::run, this);
}

void ReplicaLoopQueue::stop() {
    {
        std::lock_guard lock(mutex_);
        running_ = false;
        tasks_.clear();
    }
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

bool ReplicaLoopQueue::post_write(ProposeParam param, CompletionManager::Ticket ticket) {
    std::lock_guard lock(mutex_);
    if (!running_) {
        return false;
    }
    // 当是空的，或者尾部不是WritesTask，或者尾部是WritesTask但是满了已经，就再创建个新的，否则就和之前的合并在一起。
    if (tasks_.empty() || !std::holds_alternative<WritesTask>(tasks_.back()) ||
        std::get<WritesTask>(tasks_.back()).items.size() >= max_batch_size_) {
        tasks_.emplace_back(WritesTask{});
    }
    std::get<WritesTask>(tasks_.back()).items.push_back(ProposalItem{std::move(param), ticket});
    cv_.notify_one();
    return true;
}

bool ReplicaLoopQueue::post_task(Task task) {
    if (!task) {
        return false;
    }
    std::lock_guard lock(mutex_);
    if (!running_) {
        return false;
    }
    tasks_.emplace_back(std::move(task));
    cv_.notify_one();
    return true;
}

void ReplicaLoopQueue::run() {
    while (true) {
        std::variant<WritesTask, Task> next;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this]() { return !running_ || !tasks_.empty(); });
            if (!running_) {
                return;
            }
            next = std::move(tasks_.front());
            tasks_.pop_front();
        }
        if (auto* batch = std::get_if<WritesTask>(&next)) {
            Status status = tracker_.submit_batch(std::move(batch->items));
            if (status.fail()) {
                LOG_WARN("proposal batch failed, status:{}", status.to_string());
            }
        } else {
            std::get<Task>(next)();
        }
    }
}

}  // namespace adviskv::storage
