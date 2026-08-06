#include "e2e_context.h"

#include <fmt/core.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>
#include <utility>

#include "e2e_assert.h"
#include "meta/model/model.h"
#include "sdk/model.h"

namespace adviskv::e2e {

E2EContext::E2EContext(const Options& options) : options_(options) {
    const std::string meta_target =
        fmt::format("{}:{}", options_.meta_host, options_.meta_port);
    const std::string topo_target =
        fmt::format("{}:{}", options_.topo_host, options_.topo_port);

    print_step(fmt::format("meta target: {}", meta_target));
    print_step(fmt::format("topo target: {}", topo_target));

    auto meta_channel =
        grpc::CreateChannel(meta_target, grpc::InsecureChannelCredentials());
    meta_stub_ = meta_rpc::MetaService::NewStub(meta_channel);

    auto topo_channel =
        grpc::CreateChannel(topo_target, grpc::InsecureChannelCredentials());
    topo_stub_ = topo_rpc::TopoService::NewStub(topo_channel);
}

}  // namespace adviskv::e2e