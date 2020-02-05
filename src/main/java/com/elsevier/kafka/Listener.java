package com.elsevier.kafka;

import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;

public class Listener {

	final CountDownLatch latch1 = new CountDownLatch(1);

	@KafkaListener(id = "foo", topics = "my_topic")
	public void listen(String foo) {
		System.out.println("Message received!");
		this.latch1.countDown();
	}
}
