package hht.dragon;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeperJava Api.
 *
 * @author: huang
 * @Date: 18-7-3
 */
public class TestZooKeeper {

    final CountDownLatch latch = new CountDownLatch(1);

    final static String ZNODE_PATH = "/FirstNode_1";

    /**
     * 连接ZooKeeper。
     * @param host
     * @return
     */
    public ZooKeeper connect(String host) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper(host, 5000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        });
        latch.await();
        return zooKeeper;
    }

    /**
     * 释放资源.
     * @param zooKeeper
     */
    public void close(ZooKeeper zooKeeper) {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建Znode.
     */
    @Test
    public void createZnode() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zoo = connect("localhost");
        byte[] data = "Java API Test".getBytes();
        // 调用create方法创建Znode
        zoo.create(ZNODE_PATH, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        close(zoo);
    }

    /**
     * 判断Znode是否存在.
     */
    @Test
    public void exists() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zoo = connect("localhost");
        Stat ok = zoo.exists(ZNODE_PATH, true);
        if (ok != null) {
            System.out.println(ok.getVersion());
        }
        close(zoo);
    }

    /**
     * 获取数据.
     */
    @Test
    public void getData() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zoo = connect("localhost");
        byte[] data = zoo.getData(ZNODE_PATH, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.None) {
                    switch (watchedEvent.getState()) {
                        case Expired:
                            latch.countDown();
                            break;
                    }
                } else {
                    String path = "/MyFirstZnode";
                    try {
                        byte[] bn = zoo.getData(path,
                                false, null);
                        String data = new String(bn,
                                "UTF-8");
                        System.out.println(data);
                        latch.countDown();

                    } catch(Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }
            }
        }, null);
        System.out.println(new String(data, "UTF-8"));
        close(zoo);
    }

    /**
     * 修改数据.
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void setData() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zoo = connect("localhost");
        byte[] data = "修改数据".getBytes();
        zoo.setData(ZNODE_PATH, data, zoo.exists(ZNODE_PATH, true).getVersion());
        close(zoo);
    }

    /**
     * 获取子节点.
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void getChild() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zoo = connect("localhost");
        List<String> children = zoo.getChildren("/FirstNode", false);
        children.forEach(System.out::println);
        close(zoo);
    }

    /**
     * 删除节点.
     */
    @Test
    public void del() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zoo = connect("localhost");
        zoo.delete(ZNODE_PATH, zoo.exists(ZNODE_PATH, true).getVersion());
        close(zoo);
    }

}
