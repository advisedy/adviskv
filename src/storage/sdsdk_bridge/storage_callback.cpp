#include "storage/sdsdk_bridge/storage_callback.h"

#include "common/log.h"

namespace adviskv::storage {

Status StorageCallback::create_replica(const sdsdk::CreateReplicaArgs& args,
                                       sdsdk::CreateReplicaResult& out) {
    ReplicaInitParam param;
    param.replica_id = args.key;
    param.engine_type = args.engine_type;
    param.data_dir = replica_manager_->get_data_dir();

    Status status = replica_manager_->add_replica(param);
    if (status.ok()) {
        LOG_INFO(
            "create_replica success, table_id={}, shard_index={}, "
            "replica_index={}",
            args.key.table_id, args.key.shard_index, args.key.replica_index);
    } else {
        LOG_WARN(
            "create_replica failed, table_id={}, shard_index={}, "
            "replica_index={}, msg={}",
            args.key.table_id, args.key.shard_index, args.key.replica_index,
            status.msg());
    }
    return status;
}

Status StorageCallback::delete_replica(const sdsdk::DeleteReplicaArgs& args) {
    Status status = replica_manager_->delete_replica(args.key);
    if (status.ok()) {
        LOG_INFO(
            "delete_replica success, table_id={}, shard_index={}, "
            "replica_index={}",
            args.key.table_id, args.key.shard_index, args.key.replica_index);
    } else {
        LOG_WARN(
            "delete_replica failed, table_id={}, shard_index={}, "
            "replica_index={}, msg={}",
            args.key.table_id, args.key.shard_index, args.key.replica_index,
            status.msg());
    }
    return status;
}

Status StorageCallback::change_replica_role(
    const sdsdk::ChangeReplicaRoleArgs& args,
    sdsdk::ChangeReplicaRoleResult& out) {
    Replica* replica = replica_manager_->get_replica_by_id(args.key);
    if (!replica) {
        return Status{StatusCode::REPLICA_NOT_FOUND, "replica not found"};
    }

    LOG_INFO(
        "change_replica_role, table_id={}, shard_index={}, old_role={}, "
        "new_role={}",
        args.key.table_id, args.key.shard_index,
        static_cast<int>(args.old_role), static_cast<int>(args.new_role));
    return Status::OK();
}

}  // namespace adviskv::storage
