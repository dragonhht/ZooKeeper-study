package hht.dragon.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 使用Curator实现分布式锁.
 *
 * InterProcessMutex：分布式可重入排它锁
 * InterProcessSemaphoreMutex：分布式排它锁
 * InterProcessReadWriteLock：分布式读写锁
 * InterProcessMultiLock：将多个锁作为单个实体管理的容器
 *
 * @author: huang
 * @Date: 2019-5-15
 */
public class DistributedLockByCurator {

    /** 根节点. */
    private static final String ROOT_NODE = "/DISTRIBUTE_LOCK";

    private CuratorFramework curator;
    /** 可重入排它锁. */
    private InterProcessMutex interProcessMutex;

    /**
     * 初始化
     */
    private void init() {
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
     * 加锁
     * @return
     */
    public boolean lock() {
        if (curator == null) {
            init();
        }
        interProcessMutex = new InterProcessMutex(curator, ROOT_NODE);
        try {
            // 加锁
            interProcessMutex.acquire();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 解锁.
     * @return
     */
    public boolean unlock() {
        try {
            interProcessMutex.release();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(20);
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                DistributedLockByCurator lock = null;

                try {
                    lock = new DistributedLockByCurator();
                    countDownLatch.countDown();
                    countDownLatch.await();

                    // 获取锁
                    lock.lock();
                    TimeUnit.SECONDS.sleep(random.nextInt(5));
                } catch (InterruptedException e) {
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
