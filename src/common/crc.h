#pragma once

#include <cstdint>
#include <cstddef>

namespace adviskv {

inline uint32_t compute_crc32(const uint8_t* data, size_t len) {
    static uint32_t crc32_tab[256];
    static bool initialized = false;
    if (!initialized) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (int j = 0; j < 8; j++) {
                if (crc & 1)
                    crc = (crc >> 1) ^ 0xEDB88320;
                else
                    crc >>= 1;
            }
            crc32_tab[i] = crc;
        }
        initialized = true;
    }

    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32_tab[(crc ^ data[i]) & 0xFF];
    }
    return crc ^ 0xFFFFFFFF;
}

}  // namespace adviskv
