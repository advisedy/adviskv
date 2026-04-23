#include "storage/handler/storage_service.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/replica/replica.h"
#include <grpcpp/support/status.h>

namespace adviskv::storage{

grpc::Status StorageServiceImpl::Put(grpc::ServerContext* context,
        const rpc::PutRequest* request,
        rpc::PutResponse* response) {

        if(!replica_manager_){
            WARN("replica manager is nullptr");
            fill_base_rsp(response, Status{StatusCode::REPLICA_MANAGER_NOT_FOUND, "replica manager not found"});
            return grpc::Status::OK;
        }

        const ShardID shard_id{
            .table_id = request->table_id(),
            .shard_index = request->shard_id(),
        };
        Replica* replica = replica_manager_->get_replica(shard_id);

        if(!replica){
            WARN("replica not found, table_id = {}, shard_id = {}", request->table_id(), request->shard_id());
            fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND, "replica not found"});
            return grpc::Status::OK;
        }
        PutParam param{
            .key = request->key(),
            .value = request->value()
        };
        Status status = replica->put(param);

        if(!status.ok()){
            WARN("replica put failed, table_id = {}, shard_id = {}, key = {}, value = {}, msg = {}",
                request->table_id(), request->shard_id(), request->key(), request->value(), status.msg());
        }

        fill_base_rsp(response, status);
        return grpc::Status::OK;


    }

grpc::Status StorageServiceImpl::Get(grpc::ServerContext* context,
        const rpc::GetRequest* request,
        rpc::GetResponse* response) {

            const ShardID shard_id{
                .table_id = request->table_id(),
                .shard_index = request->shard_id(),
            };
            Replica* replica = replica_manager_->get_replica(shard_id);

            if(!replica){
                WARN("replica not found, table_id = {}, shard_id = {}", request->table_id(), request->shard_id());
                fill_base_rsp(response, Status{StatusCode::REPLICA_NOT_FOUND, "replica not found"});
                return grpc::Status::OK;
            }
            
            GetParam param{
                .key = request->key()
            };
            Value value;
            
            Status status = replica->get(param, value);
    
            if(!status.ok()){
                WARN("replica get failed, table_id = {}, shard_id = {}, key = {}, msg = {}",
                    request->table_id(), request->shard_id(), request->key(), status.msg());
            }
            fill_base_rsp(response, status);
            if(status.fail()){
                return grpc::Status::OK;
            }
            response->set_value(value);

            return grpc::Status::OK;
    
    }


grpc::Status StorageServiceImpl::Delete(grpc::ServerContext* context,
        const rpc::DeleteRequest* request,
        rpc::DeleteResponse* response) {
                // Replica* replica = replica_manager_->get_replica(request->table_id(), request->shard_id());
    
                // if(!replica){
                //     WARN("replica not found, table_id = {}, shard_id = {}", request->table_id(), request->shard_id());
                //     fill_base_rsp(*response, Status{StatusCode::REPLICA_NOT_FOUND, "replica not found"});
                //     return grpc::Status::OK;
                // }
        
                // Status status = replica->del(*request);
        
                // if(!status.ok()){
                //     WARN("replica del failed, table_id = {}, shard_id = {}, key = {}, msg = {}",
                //         request->table_id(), request->shard_id(), request->key(), status.msg());
                // }
        
                // return grpc::Status::OK;
        }

grpc::Status StorageServiceImpl::CreateReplica(grpc::ServerContext* context,
            const rpc::CreateReplicaRequest* request,
            rpc::CreateReplicaResponse* response) {


            
            return grpc::Status::OK;
        }

grpc::Status StorageServiceImpl::DeleteReplica(grpc::ServerContext* context,
            const rpc::DeleteReplicaRequest* request,
            rpc::DeleteReplicaResponse* response) {

            }

grpc::Status StorageServiceImpl::GetReplicaInfo(grpc::ServerContext* context,
                const rpc::GetReplicaInfoRequest* request,
                rpc::GetReplicaInfoResponse* response) {

                }

}
