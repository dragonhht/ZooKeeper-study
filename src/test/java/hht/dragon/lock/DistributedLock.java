package hht.dragon.lock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 分布式锁(共享锁).
 *
 * @author: huang
 * @Date: 2019-5-13
 */
public class DistributedLock {

    /** 根节点. */
    private static final String ROOT_NODE = "/DISTRIBUTE_LOCK";
    /** 锁节点Id. */
    private String lockId = "";
    private ZooKeeper zooKeeper;
    private int sessionTimeOut;

    /** 节点数据. */
    private final static byte[] data = "true".getBytes();

    final CountDownLatch latch = new CountDownLatch(1);

    public DistributedLock() throws IOException, InterruptedException {
        this.zooKeeper = ZkClient.connect();
        this.sessionTimeOut = zooKeeper.getSessionTimeout();
    }

    /**
     * 获取锁.
     * @return
     */
    public boolean lock() {
        try {
            // 创建临时有序节点(节点名称后带`/`让编号再`/`后面)
            lockId = zooKeeper.create(ROOT_NODE + "/", data,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + "成功创建lock节点: " + lockId);

            // 获取根节点下的所有子节点判断是否获取锁
            List<String> childrenNodes = zooKeeper.getChildren(ROOT_NODE, true);
            // 从小到大排序(通过SortedSet实现)
            TreeSet<String> treeSet = new TreeSet<>(childrenNodes);

            String first = treeSet.first();
            if (lockId.equals(first)) {
                // 表示当前就是最小的节点
                System.out.println(Thread.currentThread().getName() + "成功获取锁, 节点为： " + lockId);
                return true;
            }
            // 获取比当前节点更小的上个节点
            SortedSet<String> lessLocks = treeSet.headSet(lockId);
            if (!lessLocks.isEmpty()) {
                String preLick = lessLocks.last();
                zooKeeper.exists(preLick, (watchedEvent -> {
                    // 删除事件
                    if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                        latch.countDown();
                    }
                }));
                latch.await(sessionTimeOut, TimeUnit.MILLISECONDS);
                // 如果节点被删除或会话超时
                System.out.println(Thread.currentThread().getName() + "成功获取锁");
                return true;
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 释放锁
     * @return
     */
    public boolean unlock() {
        System.out.println(Thread.currentThread().getName() + "开始释放锁: " + lockId);
        try {
            // 删除节点
            zooKeeper.delete(lockId, -1);
            System.out.println(Thread.currentThread().getName() + "锁释放成功");
            return true;
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(20);
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                DistributedLock lock = null;

                try {
                    lock = new DistributedLock();
                    countDownLatch.countDown();
                    countDownLatch.await();

                    // 获取锁
                    lock.lock();
                    TimeUnit.SECONDS.sleep(random.nextInt(5));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    if (lock != null) {
                        // 释放锁
                        lock.unlock();
                    }
                }
            }).start();
        }
    }
}
