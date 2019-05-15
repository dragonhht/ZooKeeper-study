package hht.dragon;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Curator客户端.
 *
 * @author: huang
 * @Date: 2019-5-15
 */
public class CuratorTest {

    private CuratorFramework curator;

    private static final String NODE_PATH = "/test";

    private byte[] data = "true".getBytes();

    /**
     * 实例化Curator
     */
    @Before
    public void init() {
        // 1、通过CuratorFrameworkFactory创建,retryPolicy为重试机制
        /*curator = CuratorFrameworkFactory.newClient("localhost:2181",
                new RetryNTimes(10, 5000));
        curator.start();*/
        // 2、通过链式调用初始化
        curator = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new RetryNTimes(10, 5000))
                .build();
        curator.start();
    }

    @Test
    public void operate() throws Exception {
        // 创建节点数据
        curator.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(NODE_PATH, data);

        // 判断节点是否存在
        Stat stat = curator.checkExists().forPath(NODE_PATH);
        if (stat != null) {
            System.out.println("节点 " + NODE_PATH + " 存在");
        }

        // 获取节点数据
        byte[] value = curator.getData().forPath(NODE_PATH);
        System.out.println(new String(value));

        // 节点事件监听
        nodeListen();

        // 修改节点数据
        curator.setData().forPath(NODE_PATH, "hello".getBytes());
        print(NODE_PATH);

        // 获取子节点
        List<String> childrenNodes = curator.getChildren().forPath(NODE_PATH);
        System.out.println(Arrays.toString(childrenNodes.toArray()));

        // 子节点事件监听
        childrenNodeListen();

        // 删除节点(包含子节点)
        curator.delete()
                .guaranteed()
                .deletingChildrenIfNeeded()
                .withVersion(-1)
                .forPath(NODE_PATH);
    }

    /**
     * 节点事件监听
     */
    private void nodeListen() throws Exception {
        NodeCache nodeCache = new NodeCache(curator, NODE_PATH);
        nodeCache.start();
        nodeCache.getListenable().addListener(() -> {
            byte[] nowData = nodeCache.getCurrentData().getData();
            System.out.println("节点事件监听: " + new String(nowData));
        });
    }

    /**
     * 子节点事件监听
     */
    private void childrenNodeListen() throws Exception {
        PathChildrenCache childrenCache = new PathChildrenCache(curator, NODE_PATH, true);
        childrenCache.start();
        childrenCache.getListenable().addListener(((curatorFramework, pathChildrenCacheEvent) -> {
            PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
            System.out.println("子节点事件监听: " + type);
        }));
    }

    /**
     * 打印节点数据
     * @param node
     * @throws Exception
     */
    private void print(String node) throws Exception {
        byte[] value = curator.getData().forPath(NODE_PATH);
        System.out.println(new String(value));
    }
}
