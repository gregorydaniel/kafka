package com.elsevier.kafka;


import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.elsevier.kafka.test.ConsumerRecordAssert;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@EmbeddedKafka(topics = KafkaTemplateTest.TEMPLATE_TOPIC, partitions = 1)
class KafkaTemplateTest {

	static final String TEMPLATE_TOPIC = "templateTopic";

	/**
	 * @noinspection SpringJavaInjectionPointsAutowiringInspection
	 */
	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Test
	void testTemplate() throws Exception {
		BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();

		// operation for the consumer to execute
		Consumer<ConsumerRecord<Integer, String>> operation = record -> {
			System.out.println(record);
			records.add(record);
		};

		KafkaMessageListenerContainer<Integer, String> container = createMessageListenerContainer(operation);

		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);

		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(TEMPLATE_TOPIC);
		template.sendDefault("foo");
		ConsumerRecordAssert.assertThat(records.poll(10, TimeUnit.SECONDS)).hasValue("foo");

		template.sendDefault(0, 2, "bar");
		ConsumerRecord<Integer, String> received = records.poll(10, TimeUnit.SECONDS);
		ConsumerRecordAssert.assertThat(received).hasKey(2);
		ConsumerRecordAssert.assertThat(received).hasPartition(0);
		ConsumerRecordAssert.assertThat(received).hasValue("bar");

		template.send(TEMPLATE_TOPIC, 0, 2, "baz");

		received = records.poll(10, TimeUnit.SECONDS);
		ConsumerRecordAssert.assertThat(received).hasKey(2);
		ConsumerRecordAssert.assertThat(received).hasPartition(0);
		ConsumerRecordAssert.assertThat(received).hasValue("baz");
	}

	private KafkaMessageListenerContainer<Integer, String> createMessageListenerContainer(
			Consumer<ConsumerRecord<Integer, String>> operation) {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		ContainerProperties cp = new ContainerProperties(TEMPLATE_TOPIC);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf, cp);
		container.setupMessageListener(Kafka.toMessageListener(operation));
		return container;
	}
}
