#pragma once

#include <type_traits>

namespace adviskv {

template <typename T>
class IDAllocator {
    static_assert(std::is_integral_v<T>, "IDAllocator<T> requires integral T");

public:
    IDAllocator() = default;
    explicit IDAllocator(T start_id) : cur_id_(start_id) {}

    T get_next_id() { return cur_id_++; }

    T current_id() const { return cur_id_; }

private:
    T cur_id_{0};
};

}  // namespace adviskv