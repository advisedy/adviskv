#pragma once

#include <memory>
#include <vector>

#include "common/status.h"
#include "sdsdk/type.h"

namespace adviskv::sdsdk {

class IStorageCallback {
   public:
    virtual ~IStorageCallback() = default;

   public:
    virtual Status create_replica(const CreateReplicaArgs& args,
                                  CreateReplicaResult& out) = 0;

    virtual Status delete_replica(const DeleteReplicaArgs& args) = 0;

    virtual Status change_replica_role(const ChangeReplicaRoleArgs& args,
                                       ChangeReplicaRoleResult& out) = 0;

    // virtual Status collect_node_report(NodeReport& out) {
    //     return Status::OK();
    // }

};

using StorageCallbackPtr = std::shared_ptr<IStorageCallback>;

}  // namespace adviskv::sdsdk
