package hht.dragon.pub.sub;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * 发布数据到Zookeeper.
 *
 * @author: huang
 * @Date: 2019-5-15
 */
public class PublishServer {
    private static CuratorFramework curator;

    private static final String ROOT_NODE = "/config";

    private static final String CONFIG_NODE = "/server_one";

    /**
     * 初始化
     */
    private static void init() {
        curator = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new RetryNTimes(2, 5000))
                .build();
        curator.start();

        try {
            // 创建父节点
            if (curator.checkExists().forPath(ROOT_NODE) == null) {
                curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(ROOT_NODE, "".getBytes());
            }
            // 创建需要的保存配置信息的节点
            if (curator.checkExists().forPath(ROOT_NODE + CONFIG_NODE) == null) {
                curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(ROOT_NODE + CONFIG_NODE);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发布配置信息.
     * @param property
     */
    public static void publish(PropertiesPojo property) {
        try {
            byte[] data = new ObjectMapper().writeValueAsBytes(property);
            curator.setData().forPath(ROOT_NODE + CONFIG_NODE, data);
            System.out.println("发布信息成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        init();
        publish(new PropertiesPojo("com.github.hht.zookeeper.Main", "hello world", 18));
    }

}
