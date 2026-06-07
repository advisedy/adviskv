#pragma once

#include <cstdint>

namespace adviskv {

enum class StorageReplicaStatus : int8_t {
    INITIALIZING = 0,
    RECOVERING = 1,
    READY = 2,
    FAULTED = 3,
};

inline bool decode_storage_replica_status(int32_t value,
                                          StorageReplicaStatus& out) {
    switch (value) {
        case 0:
            out = StorageReplicaStatus::INITIALIZING;
            return true;
        case 1:
            out = StorageReplicaStatus::RECOVERING;
            return true;
        case 2:
            out = StorageReplicaStatus::READY;
            return true;
        case 3:
            out = StorageReplicaStatus::FAULTED;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv
