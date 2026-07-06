#pragma once

#include <cassert>
#include <functional>

#include "common/log.h"
#include "common/model/type.h"

namespace adviskv {
// 计数触发器。limit 可以是固定值，也可以由函数动态生成。
// limit 为 0 时，下一次 tick 会立即触发；limit 不能为负数。
class TickTrigger {
   public:
    using TickLimitFunc = std::function<int32()>;

    explicit TickTrigger(int32 limit_cnt)
        : limit_cnt_(checked_limit(limit_cnt)) {}

    explicit TickTrigger(TickLimitFunc limit_func)
        : limit_func_(std::move(limit_func)) {
        assert(limit_func_);
        limit_cnt_ = make_next_limit();
    }

    // 下面返回值是bool的函数，true都是代表着触发了一次函数
    bool tick() {
        if (stop_flag_) return false;

        cur_cnt_++;
        return is_catch_up_limit();
    }

    template <typename Func>
    bool tick(Func&& on_fire) {
        // int32 cur_cnt = cur_cnt_, limit_cnt = limit_cnt_;
        bool res = tick();
        // LOG_DEBUG("[TickTrigger] one tick, cur_cnt:{}, limit_cnt:{}, res:{}",
        //           cur_cnt, limit_cnt, res);

        if (!res) return false;
        on_fire();
        return true;
    }

    bool reset() {
        stop_flag_ = false;
        cur_cnt_ = 0;
        limit_cnt_ = make_next_limit();
        return is_catch_up_limit();
    }

    bool reset(int32 limit_cnt) {
        limit_func_ = TickLimitFunc{};
        limit_cnt_ = checked_limit(limit_cnt);
        return reset();
    }

    bool reset(TickLimitFunc limit_func) {
        limit_func_ = std::move(limit_func);
        assert(limit_func_);
        return reset();
    }

    bool clear() {
        if (stop_flag_) return false;
        return reset();
    }

    void stop() { stop_flag_ = true; }

    int32 get_cur_cnt() const { return cur_cnt_; }
    int32 get_limit_cnt() const { return limit_cnt_; }

   private:
    static int32 checked_limit(int32 limit_cnt) {
        assert(limit_cnt >= 0);
        return limit_cnt;
    }

    int32 make_next_limit() const {
        if (!limit_func_) {
            return limit_cnt_;
        }
        return TickTrigger::checked_limit(limit_func_());
    }

    bool is_catch_up_limit() {
        if (cur_cnt_ < limit_cnt_) {
            return false;
        }

        cur_cnt_ = 0;
        limit_cnt_ = make_next_limit();
        return true;
    }

    bool stop_flag_{false};
    int32_t cur_cnt_{0};
    TickLimitFunc limit_func_;
    int32_t limit_cnt_{0};
};
}  // namespace adviskv