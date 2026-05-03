#pragma once

#include "sdsdk/istorage_callback.h"
namespace adviskv::storage {

class StorageCallback : public sdsdk::IStorageCallback {
    /*

        virtual Status create_replica(const CreateReplicaArgs& args,
                                      CreateReplicaResult& out) = 0;

        virtual Status delete_replica(const DeleteReplicaArgs& args) = 0;

        virtual Status change_replica_role(const ChangeReplicaRoleArgs& args,
                                           ChangeReplicaRoleResult& out) = 0;

    */

   public:
    Status create_replica(const sdsdk::CreateReplicaArgs& args,
                          sdsdk::CreateReplicaResult& out) override;

    Status delete_replica(const sdsdk::DeleteReplicaArgs& args) override;

    Status change_replica_role(const sdsdk::ChangeReplicaRoleArgs& args,
                               sdsdk::ChangeReplicaRoleResult& out) override;
};

}  // namespace adviskv::storage