#include "common/enum_convert.h"

namespace adviskv {

pb::ReplicaStatus to_pb_replica_status(storage::ReplicaStatus status) {
    switch (status) {
        case storage::ReplicaStatus::INITIALIZING:
        case storage::ReplicaStatus::RECOVERING:
            return pb::ReplicaStatus::ADDING;
        case storage::ReplicaStatus::READY:
            return pb::ReplicaStatus::READY;
        case storage::ReplicaStatus::FAULTED:
        default:
            return pb::ReplicaStatus::ERROR;
    }
}
}  // namespace adviskv