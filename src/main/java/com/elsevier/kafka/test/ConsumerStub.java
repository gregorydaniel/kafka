package com.elsevier.kafka.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * A Kafka consumer that performs some operation then returns the result to a queue, for
 * inspection in a test.
 */
public class ConsumerStub implements Listener {
	private final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();

	private Consumer<ConsumerRecord<Integer, String>> operation;

	public ConsumerStub(Consumer<ConsumerRecord<Integer, String>> operation) {
		this.operation = operation;
	}

	@Override
	@KafkaListener(id = "foo", topics = "testTopic")
	public void listen(ConsumerRecord<Integer, String> record) {
		operation.accept(record);

		records.add(record);
	}

	@Override
	public BlockingQueue<ConsumerRecord<Integer, String>> getRecords() {
		return records;
	}
}
