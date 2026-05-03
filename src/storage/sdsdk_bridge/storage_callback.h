#pragma once

#include "common/status.h"
#include "common/type.h"
#include "sdsdk/istorage_callback.h"
#include "sdsdk/type.h"
#include "storage/replica/replica_manager.h"

namespace adviskv::storage {

class StorageCallback : public sdsdk::IStorageCallback {
   public:
    explicit StorageCallback(ReplicaManager* replica_manager)
        : replica_manager_(replica_manager) {}

    Status create_replica(const sdsdk::CreateReplicaArgs& args,
                          sdsdk::CreateReplicaResult& out) override;

    Status delete_replica(const sdsdk::DeleteReplicaArgs& args) override;

    Status change_replica_role(const sdsdk::ChangeReplicaRoleArgs& args,
                               sdsdk::ChangeReplicaRoleResult& out) override;

   private:
    ReplicaManager* replica_manager_;
};

}  // namespace adviskv::storage
