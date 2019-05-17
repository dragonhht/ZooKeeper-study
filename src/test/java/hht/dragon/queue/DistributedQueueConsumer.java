package hht.dragon.queue;

import hht.dragon.common.CuratorClient;
import hht.dragon.lock.DistributedLockByCurator;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * 分布式队列(消费者).
 *
 * @author: huang
 * @Date: 2019-5-17
 */
public class DistributedQueueConsumer {

    private static final String ROOT_NODE = "/queue";

    /** 使用分布式锁. */
    private DistributedLockByCurator lock = new DistributedLockByCurator();

    public DistributedQueueConsumer() {
        DistributedLockByCurator initLock = new DistributedLockByCurator();
        try {
            // 加锁
            initLock.lock();
            listen();
            List<String> children = CuratorClient.getCurator()
                    .getChildren()
                    .forPath(ROOT_NODE);
            consumerData(children);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initLock.unlock();
        }
    }

    /**
     * 监听子节点变化
     */
    private void listen() throws Exception {
        PathChildrenCache childrenCache = new PathChildrenCache(CuratorClient.getCurator(), ROOT_NODE, false);
        childrenCache.start();
        childrenCache.getListenable().addListener((curator, event) -> {
            if (PathChildrenCacheEvent.Type.CHILD_ADDED == event.getType()
                    || PathChildrenCacheEvent.Type.CHILD_REMOVED == event.getType()) {
                try {
                    // 加锁
                    lock.lock();
                    // 获取子节点
                    List<String> childrenNodes = CuratorClient.getCurator().getChildren()
                            .forPath(ROOT_NODE);
                    consumerData(childrenNodes);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // 释放锁
                    lock.unlock();
                }
            }
        });
    }

    /**
     * 消费数据.
     * @param children
     */
    public void consumerData(List<String> children) {
        // 进行排序
        TreeSet<String> treeSet = new TreeSet<>(children);
        String node = treeSet.first();
        String nodePath = ROOT_NODE + "/" + node;
        try {
            byte[] result = CuratorClient.getCurator().getData().forPath(nodePath);
            // 删除节点
            CuratorClient.getCurator().delete().withVersion(-1).forPath(nodePath);
            System.out.println(Thread.currentThread().getName() + " 处理任务中: " + new String(result));
            System.out.println(Thread.currentThread().getName() + " 任务处理结束");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 生产者
        for (int i = 0; i < 3; i++) {
            new Thread(() -> {
                DistributedQueueProducer producer = new DistributedQueueProducer();
                while (true) {
                    String data = "hello-" + new Date();
                    producer.produce(data.getBytes());
                }
            }).start();
        }

        TimeUnit.SECONDS.sleep(2);
        // 消费者
        for (int i = 0; i < 8; i++) {
            new Thread(() -> {
                new DistributedQueueConsumer();
                while (true) {
                }
            }).start();
        }
    }

}
