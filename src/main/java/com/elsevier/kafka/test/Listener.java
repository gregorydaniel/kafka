package com.elsevier.kafka.test;

import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Listener {

	void listen(ConsumerRecord<Integer, String> record);

	BlockingQueue<ConsumerRecord<Integer, String>> getRecords();

}
