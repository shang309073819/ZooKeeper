package com.shangshang.ZooKeeper;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DistributedClient implements Runnable {
	// 超时时间
	private static final int SESSION_TIMEOUT = 5000;
	// zookeeper server列表
	private String host = "192.168.2.7:2181";
	private static String groupNode = "locks";
	private static String subNode = "sub";

	private ZooKeeper zk;
	// 当前client创建的子节点
	private String thisPath;
	// 当前client等待的子节点
	private String waitPath;

	private CountDownLatch latch = new CountDownLatch(1);

	/**
	 * 连接zookeeper
	 */
	public void connectZookeeper() throws Exception {
		zk = new ZooKeeper(host, SESSION_TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				try {
					// 连接建立时, 打开latch, 唤醒wait在该latch上的线程
					if (event.getState() == KeeperState.SyncConnected) {
						latch.countDown();
					}
					// 发生了waitPath的删除事件
					if (event.getType() == EventType.NodeDeleted
							&& event.getPath().equals(waitPath)) {
						doSomething();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		// 等待连接建立
		latch.await();

		// 查看根节点
		// System.out.println("根节点:" + zk.getChildren("/", true));

		// 创建子节点
		thisPath = zk.create("/" + groupNode + "/" + subNode, null,
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		// 查看子节点
		// System.out.println("子节点:" + zk.getChildren("/" + groupNode, true));

		// wait一小会, 让结果更清晰一些
		Thread.sleep(10);

		// 注意, 没有必要监听"/locks"的子节点的变化情况
		List<String> childrenNodes = zk.getChildren("/" + groupNode, false);

		// 列表中只有一个子节点, 那肯定就是thisPath, 说明client获得锁
		if (childrenNodes.size() == 1) {
			doSomething();
		} else {
			String thisNode = thisPath.substring(("/" + groupNode + "/")
					.length());
			// 排序
			Collections.sort(childrenNodes);
			int index = childrenNodes.indexOf(thisNode);
			if (index == -1) {
				// never happened
			} else if (index == 0) {
				// inddx == 0, 说明thisNode在列表中最小, 当前client获得锁
				doSomething();
			} else {
				// 获得排名比thisPath前1位的节点
				this.waitPath = "/" + groupNode + "/"
						+ childrenNodes.get(index - 1);
				// 在waitPath上注册监听器, 当waitPath被删除时, zookeeper会回调监听器的process方法
				System.out.println("waitPath is" + waitPath);
				zk.getData(waitPath, true, new Stat());
			}
		}
	}

	private void doSomething() throws Exception {
		try {
			System.out.println(Thread.currentThread().getName() + ", 得到锁: "
					+ thisPath);
			//Thread.sleep(2000);
		} finally {
			System.out.println(Thread.currentThread().getName() + ", 释放锁: "
					+ thisPath);
			// 将thisPath删除, 监听thisPath的client将获得通知
			// 相当于释放锁
			zk.delete(this.thisPath, -1);
			// 关闭连接
			zk.close();
		}
	}

	public void run() {
		try {
			connectZookeeper();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void init() throws Exception {
		// 创建一个与服务器的连接
		ZooKeeper zk1 = new ZooKeeper("192.168.2.7:2181", 60000, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
			}
		});
		if (zk1.exists("/" + groupNode, true) != null) {
			zk1.delete("/" + groupNode, -1);
		}
		// 创建groupNode
		if (zk1.exists("/" + groupNode, true) == null) {
			zk1.create("/" + groupNode, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		zk1.close();
	}

	public static void main(String[] args) throws Exception {

		init();
		for (int i = 0; i < 10; i++) {
			DistributedClient dl = new DistributedClient();
			Thread t = new Thread(dl, "Thread" + (i + 1));
			t.start();
		}

		// Thread.sleep(Long.MAX_VALUE);
	}
}
