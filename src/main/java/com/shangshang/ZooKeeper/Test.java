package com.shangshang.ZooKeeper;

class Ticket implements Runnable {

	public void run() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("ticket" + Thread.currentThread().getName());
	}

}

public class Test {
	public static void main(String[] args) {
		Ticket ticket = new Ticket();
		for (int i = 0; i < 10; i++) {
			Thread thread = new Thread(ticket);
			thread.start();
		}
	}
}
