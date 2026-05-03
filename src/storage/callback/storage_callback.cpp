#include "storage/callback/storage_callback.h"

#include "common/status.h"
#include "storage/model/param.h"

namespace adviskv::storage {

Status create_replica(const sdsdk::CreateReplicaArgs& args,
                      sdsdk::CreateReplicaResult& out) {
    ReplicaInitParam param;
    param.replica_id = args.key;
    param.engine_type = static_cast<EngineType>(args.engine_type);
    return Status::OK();
}

Status delete_replica(const sdsdk::DeleteReplicaArgs& args) {}

Status change_replica_role(const sdsdk::ChangeReplicaRoleArgs& args,
                           sdsdk::ChangeReplicaRoleResult& out) {}

}  // namespace adviskv::storage