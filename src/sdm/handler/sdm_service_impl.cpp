#include "sdm/handler/sdm_service_impl.h"
#include <cstddef>

namespace adviskv{

grpc::Status SdmServiceImpl::PlaceTable(grpc::ServerContext* context,
    const rpc::PlaceTableRequest* request,
        rpc::PlaceTableResponse* response) {

    PlaceTableParam param{
        .db_name = request->db_name(),
        .table_name = request->table_name(),
        .replica_count = request->replica_count(),
        .shard_count = request->shard_count(),
        .zone = request->zone()
    };

    Status status = placement_service_->place_table(param, nullptr);

    fill_base_rsp(response, status);

    return grpc::Status::OK;

}

}