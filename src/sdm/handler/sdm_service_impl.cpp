#include "sdm/handler/sdm_service_impl.h"
#include <cstddef>
#include <grpcpp/support/status.h>

namespace adviskv{

grpc::Status SdmServiceImpl::PlaceTable(grpc::ServerContext* context,
    const rpc::PlaceTableRequest* request,
        rpc::PlaceTableResponse* response) {

    PlaceTableParam param{
        .db_id = request->db_id(),
        .table_id = request->table_id(),
        .db_name = request->db_name(),
        .table_name = request->table_name(),
        .replica_count = request->replica_count(),
        .shard_count = request->shard_count(),
    };

    Status status = placement_service_->place_table(param);

    fill_base_rsp(response, status);

    return grpc::Status::OK;

}


grpc::Status SdmServiceImpl::PlaceDB(grpc::ServerContext* context,
    const rpc::PlaceDBRequest* request,
    rpc::PlaceDBResponse* response) {


        return grpc::Status::OK;
    }


}