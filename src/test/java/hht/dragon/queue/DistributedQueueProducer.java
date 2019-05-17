package hht.dragon.queue;

import hht.dragon.common.CuratorClient;
import hht.dragon.lock.DistributedLockByCurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * 分布式队列(生产者).
 *
 * @author: huang
 * @Date: 2019-5-17
 *
 */
public class DistributedQueueProducer {

    private static final String ROOT_NODE = "/queue";
    private static final String QUEUE_NODE = "/queue-";

    public DistributedQueueProducer() {
        DistributedLockByCurator lock = new DistributedLockByCurator();
        try {
            lock.lock();
            init();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private static void init() throws Exception {
        if (CuratorClient.getCurator().checkExists().forPath(ROOT_NODE) == null) {
            CuratorClient.getCurator()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ROOT_NODE, "分布式队列".getBytes());
        }
    }

    private static void produceQueue(byte[] data) {
        try {
            // 使用临时有序节点
            CuratorClient.getCurator()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(ROOT_NODE + QUEUE_NODE, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 生产数据.
     * @param data
     */
    public void produce(byte[] data) {
        try {
            produceQueue(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
