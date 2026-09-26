#include "e2e_context.h"

#include <fmt/core.h>
#include <grpcpp/create_channel.h>

#include "e2e_assert.h"

namespace adviskv::e2e {

E2EContext::E2EContext(const Options& options) : options_(options) {
    const std::string meta_target = fmt::format("{}:{}", options_.meta_host, options_.meta_port);
    print_step(fmt::format("meta target: {}", meta_target));

    auto channel = grpc::CreateChannel(meta_target, grpc::InsecureChannelCredentials());
    meta_stub_ = meta_rpc::MetaService::NewStub(channel);
}

}  // namespace adviskv::e2e
