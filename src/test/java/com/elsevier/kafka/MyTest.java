package com.elsevier.kafka;

import static com.elsevier.kafka.MyTest.TOPIC_1;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@EmbeddedKafka(topics = TOPIC_1, partitions = 1)
class MyTest {

	/**
	 * @noinspection SpringJavaInjectionPointsAutowiringInspection
	 */
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	public static final String TOPIC_1 = "topic-1";

	@Test
	public void testSimple() throws Exception {
		System.out.println("");
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = consumerFactory.createConsumer();

		ContainerProperties containerProperties = new ContainerProperties(TOPIC_1);
		KafkaMessageListenerContainer<Integer, String> messageListenerContainer =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();
		messageListenerContainer.setupMessageListener((MessageListener<Integer, String>) record -> {
			System.out.println(record);
			records.add(record);
		});
		messageListenerContainer.setBeanName("templateTests");
		messageListenerContainer.start();

		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
		DefaultKafkaProducerFactory<Integer, String> producerFactory =
				new DefaultKafkaProducerFactory<>(producerProps);

		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory);

		this.embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, TOPIC_1);

		// what is this doing?
		ContainerTestUtils.waitForAssignment(messageListenerContainer,
				embeddedKafkaBroker.getPartitionsPerTopic());

		template.send(TOPIC_1, 1, "hello world");

		ConsumerRecords<Integer, String> replies = KafkaTestUtils.getRecords(consumer);
		assertThat(replies.count()).isGreaterThanOrEqualTo(1);
	}
}
