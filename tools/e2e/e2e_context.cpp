#include "e2e_context.h"

#include <fmt/core.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>
#include <utility>

#include "e2e_assert.h"
#include "catalog/model/model.h"
#include "sdk/model.h"

namespace adviskv::e2e {

E2EContext::E2EContext(const Options& options) : options_(options) {
    const std::string catalog_target =
        fmt::format("{}:{}", options_.catalog_host, options_.catalog_port);
    const std::string topo_target =
        fmt::format("{}:{}", options_.topo_host, options_.topo_port);

    print_step(fmt::format("catalog target: {}", catalog_target));
    print_step(fmt::format("topo target: {}", topo_target));

    auto catalog_channel =
        grpc::CreateChannel(catalog_target, grpc::InsecureChannelCredentials());
    catalog_stub_ = catalog_rpc::CatalogService::NewStub(catalog_channel);

    auto topo_channel =
        grpc::CreateChannel(topo_target, grpc::InsecureChannelCredentials());
    topo_stub_ = topo_rpc::TopoService::NewStub(topo_channel);
}

}  // namespace adviskv::e2e
