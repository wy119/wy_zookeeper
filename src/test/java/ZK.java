import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @description: Zookeeper测试
 * @author:wy
 * @Date: 2019/3/31 13:54
 */
public class ZK {
    /*
    znode创建类型(CreateMode),有以下四种：

        PERSISTENT                持久化节点

        PERSISTENT_SEQUENTIAL     顺序自动编号持久化节点，这种节点会根据当前已存在的节点数自动加 1

        EPHEMERAL                 临时节点， 客户端session超时这类节点就会被自动删除

        EPHEMERAL_SEQUENTIAL      临时自动编号节点

    四种权限类型：

    OPEN_ACL_UNSAFE  : 完全开放的ACL，任何连接的客户端都可以操作该属性znode

    CREATOR_ALL_ACL : 只有创建者才有ACL权限

    READ_ACL_UNSAFE：只能读取ACL

    使用临时节点实现分布式锁，节点名称唯一，一个进程创建一个临时节点，别的就创建不了，等连接一释放，释放锁，别的进程就可以创建了。
    */

    // 连接地址
    private static final String adress = "localhost:2181";
    private static final int timeOut = 2000;

    // 信号量,阻塞程序执行,用户等待zookeeper连接成功,发送成功信号，
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 创建一个zk
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        ZooKeeper zk = new ZooKeeper(adress, timeOut, new Watcher() {

            // 监听者内部类
            public void process(WatchedEvent watchedEvent) {

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 1. 获取事件状态
                Event.KeeperState keeperState = watchedEvent.getState();
                // 2.获取事件类型
                Event.EventType eventType = watchedEvent.getType();

                // 连接状态
                if (eventType == Event.EventType.None) {
                    System.out.println("zk开始启动连接...");
                    countDownLatch.countDown();
                }
            }
        });

        // 持久节点
        String result = zk.create("/cx4", "cx".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // 临时节点,连接释放删除节点
        String temp = zk.create("/cx5", "cx".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println("创建了持久节点" + result);

        zk.close();
    }

    /*
    OPEN_ACL_UNSAFE  : 完全开放的ACL，任何连接的客户端都可以操作该属性znode

    CREATOR_ALL_ACL : 只有创建者才有ACL权限

    READ_ACL_UNSAFE：只能读取ACL

    使用临时节点实现分布式锁，节点名称唯一，一个进程创建一个临时节点，别的就创建不了，等连接一释放，释放锁，别的进程就可以创建了。
    */

}
