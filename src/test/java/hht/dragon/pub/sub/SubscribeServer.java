package hht.dragon.pub.sub;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 订阅服务.
 *
 * @author: huang
 * @Date: 2019-5-16
 */
public class SubscribeServer {

    private static CuratorFramework curator;
    private static NodeCache nodeCache;

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
    }

    /**
     * 订阅（通过监听事件）
     */
    public static void subscribe() {
        nodeCache = new NodeCache(curator, ROOT_NODE + CONFIG_NODE);

        try {
            nodeCache.start(true);
            if (nodeCache.getCurrentData() != null) {
                PropertiesPojo property = new ObjectMapper().readValue(nodeCache.getCurrentData().getData(), PropertiesPojo.class);
                System.out.println("接收到的数据为： " + property);
            }

            // 通过监听事件,监听数据发生变化
            nodeCache.getListenable().addListener(() -> {
                PropertiesPojo property = new ObjectMapper().readValue(nodeCache.getCurrentData().getData(), PropertiesPojo.class);
                System.out.println("节点数据发生变化: " + property);
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 取消订阅
     */
    public static void unSubscribe() {
        if (nodeCache != null) {
            try {
                nodeCache.close();
                System.out.println("已取消订阅");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        init();
        subscribe();
        TimeUnit.SECONDS.sleep(100);
        unSubscribe();
    }
}
