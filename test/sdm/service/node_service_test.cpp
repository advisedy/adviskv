#include "sdm/service/node_service.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/status.h"
#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {
namespace {

RegisterNodeParam make_register_node_param() {
    return RegisterNodeParam{
        .node_id = "node-a",
        .ip = "127.0.0.1",
        .port = 18080,
        .resource_pool = "pool-a",
        .dc = "dc-a",
        .last_heartbeat_ts = 123456,
    };
}

}  // namespace

// 检测合法节点注册后，NodeService 会把节点元信息写入 SdmStore。
TEST(NodeServiceTest, RegisterNodeStoresValidNodeMeta) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    NodeService service(&store);

    Status status = service.register_node(make_register_node_param());

    ASSERT_TRUE(status.ok()) << status.msg();
    NodeOr stored;
    ASSERT_TRUE(store.get_node("node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->id, "node-a");
    EXPECT_EQ(stored->spec.resource_pool, "pool-a");
    EXPECT_EQ(stored->spec.dc, "dc-a");
    EXPECT_EQ(stored->spec.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.endpoint.ip, "127.0.0.1");
    EXPECT_EQ(stored->state.endpoint.port, 18080);
    EXPECT_EQ(stored->state.last_heartbeat_ts, 123456);
}

// 检测重复注册同一个 node_id 时，新注册信息会覆盖旧节点状态。
TEST(NodeServiceTest, RegisterNodeUpdatesExistingNode) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    NodeService service(&store);
    ASSERT_TRUE(service.register_node(make_register_node_param()).ok());

    RegisterNodeParam update = make_register_node_param();
    update.ip = "10.0.0.1";
    update.port = 19090;
    update.resource_pool = "pool-b";
    update.dc = "dc-b";
    update.last_heartbeat_ts = 654321;
    Status status = service.register_node(update);

    ASSERT_TRUE(status.ok()) << status.msg();
    NodeOr stored;
    ASSERT_TRUE(store.get_node("node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->spec.resource_pool, "pool-b");
    EXPECT_EQ(stored->spec.dc, "dc-b");
    EXPECT_EQ(stored->spec.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(stored->state.endpoint.port, 19090);
    EXPECT_EQ(stored->state.last_heartbeat_ts, 654321);
}

// 检测缺少 node_id、ip 或端口非法时，NodeService 会拒绝注册。
TEST(NodeServiceTest, RegisterNodeRejectsInvalidParams) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    NodeService service(&store);

    std::vector<RegisterNodeParam> invalid_params;
    RegisterNodeParam empty_node_id = make_register_node_param();
    empty_node_id.node_id.clear();
    invalid_params.push_back(empty_node_id);

    RegisterNodeParam empty_ip = make_register_node_param();
    empty_ip.ip.clear();
    invalid_params.push_back(empty_ip);

    RegisterNodeParam invalid_port = make_register_node_param();
    invalid_port.port = 0;
    invalid_params.push_back(invalid_port);

    for (const RegisterNodeParam& param : invalid_params) {
        Status status = service.register_node(param);
        EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    }
}

}  // namespace adviskv::sdm