#include "sdm/service/node_service.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "sdm/store/sdm_store.h"
#include "sdm/sdm_store_test_helper.h"

namespace adviskv::sdm {
namespace {

RegisterNodeParam make_register_node_param() {
    return RegisterNodeParam{"node-a", "127.0.0.1", 18080,
                             "pool-a", "dc-a",      123456};
}

}  // namespace

// 检测 register_node 会把合法节点元信息写入 store，并初始化为 ONLINE。
TEST(NodeServiceTest, RegisterNodeStoresValidNodeMeta) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    NodeService service(&store);

    Status status = service.register_node(make_register_node_param());

    ASSERT_TRUE(status.ok()) << status.msg();
    NodeOr stored;
    ASSERT_TRUE(store_test::get_node(store, "node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->id, "node-a");
    EXPECT_EQ(stored->meta.resource_pool, "pool-a");
    EXPECT_EQ(stored->meta.dc, "dc-a");
    EXPECT_EQ(stored->state.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.endpoint.ip, "127.0.0.1");
    EXPECT_EQ(stored->state.endpoint.port, 18080);
    EXPECT_EQ(stored->state.last_heartbeat_ts, 123456);
}

// 检测重复注册同一个 node_id 时，会覆盖节点元信息和心跳状态。
TEST(NodeServiceTest, RegisterNodeUpdatesExistingNode) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
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
    ASSERT_TRUE(store_test::get_node(store, "node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->meta.resource_pool, "pool-b");
    EXPECT_EQ(stored->meta.dc, "dc-b");
    EXPECT_EQ(stored->state.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(stored->state.endpoint.port, 19090);
    EXPECT_EQ(stored->state.last_heartbeat_ts, 654321);
}

// 检测 register_node 会拒绝缺少 node_id、ip 或非法端口的参数。
TEST(NodeServiceTest, RegisterNodeRejectsInvalidParams) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
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
