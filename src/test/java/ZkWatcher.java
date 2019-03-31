import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ZkWatcher implements Watcher {

    // 集群连接地址
    private static final String CONNECT_ADDRES = "127.0.0.1:2181";
    // 会话超时时间
    private static final int SESSIONTIME = 2000;
    // 信号量,让zk在连接之前等待,连接成功后才能往下走.
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    private ZooKeeper zk;

    public static void main(String[] args) throws KeeperException, InterruptedException {

        ZkWatcher zkClientWatcher = new ZkWatcher();
        zkClientWatcher.createConnection(CONNECT_ADDRES, SESSIONTIME);
        boolean createResult = zkClientWatcher.createNode("/p15", "pa-644064");
        zkClientWatcher.updateNode("/pa2", "7894561");
    }

    public void createConnection(String connectAddres, int sessionTimeOut) {

        try {
            zk = new ZooKeeper(connectAddres, sessionTimeOut, this);
            System.out.println("zk 开始启动连接服务器....");
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建节点
    public boolean createNode(String path, String data) {

        try {
            this.zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("节点修改成功, Path:" + path + ",data:" + data);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // 修改节点内容
    public boolean updateNode(String path, String data) throws KeeperException, InterruptedException {

        this.zk.setData(path, data.getBytes(), -1);
        System.out.println();

        return false;
    }

    // 事件通知
    public void process(WatchedEvent watchedEvent) {

        // 获取事件状态
        KeeperState keeperState = watchedEvent.getState();
        // 获取事件类型
        EventType eventType = watchedEvent.getType();
        // zk 路径
        String path = watchedEvent.getPath();
        System.out.println("进入到 process() keeperState:" + keeperState + ", eventType:" + eventType + ", path:" + path);

        // 判断是否建立连接
        if (KeeperState.SyncConnected == keeperState) {
            if (EventType.None == eventType) {
                // 如果建立建立成功,让后程序往下走
                System.out.println("zk 建立连接成功!");
                countDownLatch.countDown();
            } else if (EventType.NodeCreated == eventType) {

                System.out.println("事件通知,新增node节点" + path);
            } else if (EventType.NodeDataChanged == eventType) {

                System.out.println("事件通知,当前node节点" + path + "被修改....");
            } else if (EventType.NodeDeleted == eventType) {

                System.out.println("事件通知,当前node节点" + path + "被删除....");
            }

        }
    }

}


