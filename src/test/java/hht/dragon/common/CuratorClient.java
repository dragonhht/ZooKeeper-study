package hht.dragon.common;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/**
 * .
 *
 * @author: huang
 * @Date: 2019-5-17
 */
public class CuratorClient {
    private static CuratorFramework curator;

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

    public static CuratorFramework getCurator() {
        if (curator ==null) {
            init();
        }
        return curator;
    }

    public static void close() {
        if (curator != null) {
            curator.close();
        }
    }
}
