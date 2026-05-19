#pragma once

#include "common/define.h"
#include <type_traits>
#include <utility>

namespace adviskv {

template <typename F>
class ScopeExit {
   public:
    explicit ScopeExit(F&& fn) : fn_(std::forward<F>(fn)) {}

    DISALLOW_COPY_AND_ASSIGN(ScopeExit)

    ScopeExit(ScopeExit&& other) noexcept
        : fn_(std::move(other.fn_)), active_(other.active_) {
        other.active_ = false;
    }

    ~ScopeExit() {
        if (active_) {
            fn_();
        }
    }

    void cancel() { active_ = false; }

   private:
    F fn_;
    bool active_{true};
};

template <typename F>
auto Defer(F&& fn) {
    return ScopeExit<std::decay_t<F>>(std::forward<F>(fn));
}

}  // namespace adviskv