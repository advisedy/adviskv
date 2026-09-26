#include "storage/replica/completion_manager.h"

namespace adviskv::storage {

CompletionManager::Ticket CompletionManager::create() {
    std::lock_guard lock(mutex_);
    Ticket ticket = next_ticket_++;
    pending_.try_emplace(ticket);
    return ticket;
}

Status CompletionManager::wait(Ticket ticket) {
    std::unique_lock lock(mutex_);
    auto it = pending_.find(ticket);
    if (it == pending_.end()) {
        return Status::ERROR("completion ticket is missing");
    }
    Pending& pending = it->second;
    pending.cv.wait(lock, [&]() { return pending.done; });
    Status status = pending.status;
    pending_.erase(ticket);
    return status;
}

Status CompletionManager::wait_for(Ticket ticket, Milliseconds timeout, const Status& timeout_status) {
    std::unique_lock lock(mutex_);
    auto it = pending_.find(ticket);
    if (it == pending_.end()) {
        return Status::ERROR("completion ticket is missing");
    }
    Pending& pending = it->second;
    if (!pending.cv.wait_for(lock, timeout, [&]() { return pending.done; })) {
        pending_.erase(ticket);
        return timeout_status;
    }
    Status status = pending.status;
    pending_.erase(ticket);
    return status;
}

bool CompletionManager::finish(Ticket ticket, const Status& status) {
    std::unique_lock lock(mutex_);
    auto it = pending_.find(ticket);
    if (it == pending_.end() || it->second.done) {
        return false;
    }
    Pending& pending = it->second;
    pending.status = status;
    pending.done = true;
    pending.cv.notify_one();
    return true;
}

void CompletionManager::fail_all(const Status& status) {
    std::lock_guard lock(mutex_);
    for (auto& [_, pending] : pending_) {
        if (pending.done) {
            continue;
        }
        pending.status = status;
        pending.done = true;
        pending.cv.notify_one();
    }
}

}  // namespace adviskv::storage
