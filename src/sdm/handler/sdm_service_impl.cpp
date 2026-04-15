#include "sdm/handler/sdm_service_impl.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"
#include <cstddef>
#include <grpcpp/support/status.h>

namespace adviskv::sdm {

grpc::Status SdmServiceImpl::PlaceTable(grpc::ServerContext *context,
                                        const rpc::PlaceTableRequest *request,
                                        rpc::PlaceTableResponse *response) {

  PlaceTableParam param{
      .db_id = request->db_id(),
      .table_id = request->table_id(),
      .db_name = request->db_name(),
      .table_name = request->table_name(),
      .replica_count = request->replica_count(),
      .shard_count = request->shard_count(),
      .resource_pool = request->resource_pool(),
  };

  Status status = table_service_->place_table(param);

  fill_base_rsp(response, status);

  return grpc::Status::OK;
}

grpc::Status SdmServiceImpl::PlaceDB(grpc::ServerContext *context,
                                     const rpc::PlaceDBRequest *request,
                                     rpc::PlaceDBResponse *response) {

  return grpc::Status::OK;
}

} // namespace adviskv::sdm