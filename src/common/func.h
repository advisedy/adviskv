
#include <cstdint>
#include <chrono>


inline int64_t get_current_ts_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}