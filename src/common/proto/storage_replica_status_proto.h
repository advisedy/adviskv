#pragma once

#include "common.pb.h"
#include "common/model/storage_replica_status.h"

namespace adviskv {

inline bool decode_pb_storage_replica_status(pb::StorageReplicaStatus in,
                                             StorageReplicaStatus& out) {
    switch (in) {
        case pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_INITIALIZING:
            out = StorageReplicaStatus::INITIALIZING;
            return true;
        case pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_RECOVERING:
            out = StorageReplicaStatus::RECOVERING;
            return true;
        case pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_READY:
            out = StorageReplicaStatus::READY;
            return true;
        case pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_FAULTED:
            out = StorageReplicaStatus::FAULTED;
            return true;
        case pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_UNSPECIFIED:
        default:
            return false;
    }
}

inline bool encode_pb_storage_replica_status(StorageReplicaStatus in,
                                             pb::StorageReplicaStatus& out) {
    switch (in) {
        case StorageReplicaStatus::INITIALIZING:
            out = pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_INITIALIZING;
            return true;
        case StorageReplicaStatus::RECOVERING:
            out = pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_RECOVERING;
            return true;
        case StorageReplicaStatus::READY:
            out = pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_READY;
            return true;
        case StorageReplicaStatus::FAULTED:
            out = pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_FAULTED;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv
