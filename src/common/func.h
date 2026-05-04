#pragma once
#include <cstdint>
#include <chrono>
#include <random>
namespace adviskv{


inline int64_t get_current_ts_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline int32_t get_random_int32(int32_t down, int32_t up){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dist(down, up);
    return dist(gen);
}

template <typename T, typename U>
inline void ad_erase_if(std::vector<T>& a, U f) {
    for (auto it = a.begin(); it != a.end();) {
        if (f(*it)) {
            it = a.erase(it);
        } else {
            it++;
        }
    }
}

}
