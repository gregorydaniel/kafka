package com.elsevier.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.elsevier.kafka.test.ConsumerStub;
import com.elsevier.kafka.test.Listener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@Configuration
@EnableKafka
public class KafkaConfiguration {

	/**
	 * @noinspection SpringJavaInjectionPointsAutowiringInspection
	 */
	@Bean
	public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(EmbeddedKafkaBroker embeddedKafka) {
		Map<String, Object> consumerProps = KafkaConfiguration.consumerProps(embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
		ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		return factory;
	}

	@Bean
	public Listener listener() {
		return new ConsumerStub(operation());
	}

	private static Consumer<ConsumerRecord<Integer, String>> operation() {
		return System.out::println;
	}

	private static Map<String, Object> consumerProps(EmbeddedKafkaBroker embeddedKafka) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, 1);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return props;
	}
}