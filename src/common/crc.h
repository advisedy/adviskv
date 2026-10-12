#pragma once

#include <cstddef>
#include <cstdint>

namespace adviskv {

namespace crc_detail {

inline constexpr uint32_t K_CRC32_POLYNOMIAL = 0xEDB88320;

inline uint32_t* crc32_table() {
    static uint32_t tab[256];
    static bool initialized = false;
    if (!initialized) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (int j = 0; j < 8; j++) {
                if (crc & 1)
                    crc = (crc >> 1) ^ K_CRC32_POLYNOMIAL;
                else
                    crc >>= 1;
            }
            tab[i] = crc;
        }
        initialized = true;
    }
    return tab;
}

}  // namespace crc_detail

inline uint32_t update_crc32(uint32_t crc, const uint8_t* data, size_t len) {
    uint32_t* tab = crc_detail::crc32_table();
    uint32_t state = ~crc;
    for (size_t i = 0; i < len; i++) {
        state = (state >> 8) ^ tab[(state ^ data[i]) & 0xFF];
    }
    return ~state;
}

inline uint32_t compute_crc32(const uint8_t* data, size_t len) { return update_crc32(0, data, len); }

}  // namespace adviskv
