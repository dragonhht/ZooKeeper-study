package hht.dragon.lock;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 客户端连接.
 *
 * @author: huang
 * @Date: 2019-5-13
 */
public class ZkClient {

    private static final String CONNECT_STRING = "localhost:2181";

    private static int sessionTimeOut;

    /**
     * 获取连接
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static ZooKeeper connect() throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(CONNECT_STRING, 5000, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                latch.countDown();
                System.out.println("状态： " + watchedEvent.getState());
            }
        });
        latch.await();
        return zooKeeper;
    }

    public int getSessionTimeOut() {
        return sessionTimeOut;
    }

    public static void setSessionTimeOut(int sessionTimeOut) {
        ZkClient.sessionTimeOut = sessionTimeOut;
    }

    @Test
    public void main() throws IOException, InterruptedException {
        System.out.println(connect());
    }

}
