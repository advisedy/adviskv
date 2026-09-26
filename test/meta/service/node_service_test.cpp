#include "meta/service/node_service.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "meta/store/meta_store.h"
#include "test/meta/store/meta_store_test_helper.h"

namespace adviskv::meta {
namespace {

RegisterNodeParam make_register_node_param() {
    return RegisterNodeParam{"node-a", "127.0.0.1", 18080,
                             "pool-a", "dc-a",      123456};
}

}  // namespace

// 检测 register_node 会把合法节点元信息写入 store，并初始化为 ONLINE。
TEST(NodeServiceTest, RegisterNodeStoresValidNodeMeta) {
    MetaStore store{MemoryMetaStoreParam{}};
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

// 检测相同拓扑重复注册时，只更新节点运行态信息。
TEST(NodeServiceTest, RegisterNodeUpdatesExistingEndpointForSameTopology) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    NodeService service(&store);
    ASSERT_TRUE(service.register_node(make_register_node_param()).ok());

    RegisterNodeParam update = make_register_node_param();
    update.ip = "10.0.0.1";
    update.port = 19090;
    update.last_heartbeat_ts = 654321;
    Status status = service.register_node(update);

    ASSERT_TRUE(status.ok()) << status.msg();
    NodeOr stored;
    ASSERT_TRUE(store_test::get_node(store, "node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->meta.resource_pool, "pool-a");
    EXPECT_EQ(stored->meta.dc, "dc-a");
    EXPECT_EQ(stored->state.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.offline_reason, NodeOfflineReason::NONE);
    EXPECT_EQ(stored->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(stored->state.endpoint.port, 19090);
    EXPECT_EQ(stored->state.last_heartbeat_ts, 654321);
}

// 检测拓扑不一致的重复注册会保留 Meta 基准并将节点置为不可用。
TEST(NodeServiceTest, RegisterNodeMarksTopologyMismatchOffline) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    NodeService service(&store);
    ASSERT_TRUE(service.register_node(make_register_node_param()).ok());

    RegisterNodeParam update = make_register_node_param();
    update.ip = "10.0.0.1";
    update.port = 19090;
    update.resource_pool = "pool-b";
    update.dc = "dc-b";
    update.last_heartbeat_ts = 654321;
    ASSERT_TRUE(service.register_node(update).ok());

    NodeOr stored;
    ASSERT_TRUE(store_test::get_node(store, "node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->meta.resource_pool, "pool-a");
    EXPECT_EQ(stored->meta.dc, "dc-a");
    EXPECT_EQ(stored->state.status, NodeStatus::OFFLINE);
    EXPECT_EQ(stored->state.offline_reason, NodeOfflineReason::TOPOLOGY_MISMATCH);
    EXPECT_EQ(stored->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(stored->state.endpoint.port, 19090);
    EXPECT_EQ(stored->state.last_heartbeat_ts, 654321);
}

// 检测拓扑恢复一致后，注册可以清除拓扑异常并恢复节点。
TEST(NodeServiceTest, RegisterNodeRestoresTopologyMismatchNode) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    NodeService service(&store);
    ASSERT_TRUE(service.register_node(make_register_node_param()).ok());

    RegisterNodeParam mismatch = make_register_node_param();
    mismatch.resource_pool = "pool-b";
    ASSERT_TRUE(service.register_node(mismatch).ok());

    RegisterNodeParam recovered = make_register_node_param();
    recovered.ip = "10.0.0.2";
    recovered.port = 19091;
    recovered.last_heartbeat_ts = 765432;
    ASSERT_TRUE(service.register_node(recovered).ok());

    NodeOr stored;
    ASSERT_TRUE(store_test::get_node(store, "node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->meta.resource_pool, "pool-a");
    EXPECT_EQ(stored->meta.dc, "dc-a");
    EXPECT_EQ(stored->state.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.offline_reason, NodeOfflineReason::NONE);
    EXPECT_EQ(stored->state.endpoint.ip, "10.0.0.2");
    EXPECT_EQ(stored->state.endpoint.port, 19091);
}

// 检测 register_node 会拒绝缺少 node_id、ip、拓扑信息或非法端口的参数。
TEST(NodeServiceTest, RegisterNodeRejectsInvalidParams) {
    MetaStore store{MemoryMetaStoreParam{}};
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

    RegisterNodeParam empty_resource_pool = make_register_node_param();
    empty_resource_pool.resource_pool.clear();
    invalid_params.push_back(empty_resource_pool);

    RegisterNodeParam empty_dc = make_register_node_param();
    empty_dc.dc.clear();
    invalid_params.push_back(empty_dc);

    for (const RegisterNodeParam& param : invalid_params) {
        Status status = service.register_node(param);
        EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    }
}

}  // namespace adviskv::meta
