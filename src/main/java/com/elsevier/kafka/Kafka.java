package com.elsevier.kafka;

import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class Kafka {

	public static <T, V> MessageListener<T, V> toMessageListener(Consumer<ConsumerRecord<T, V>> operation) {
		//noinspection Convert2MethodRef
		return record -> operation.accept(record);

	}

}
