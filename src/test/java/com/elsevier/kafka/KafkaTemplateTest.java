package com.elsevier.kafka;


import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.elsevier.kafka.test.ConsumerRecordAssert;
import com.elsevier.kafka.test.Listener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@SpringBootTest
@EmbeddedKafka(topics = KafkaTemplateTest.TOPIC, partitions = 1)
@ContextConfiguration(classes = KafkaConfiguration.class)
class KafkaTemplateTest {

	static final String TOPIC = "testTopic";

	/**
	 * @noinspection SpringJavaInjectionPointsAutowiringInspection
	 */
	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	/**
	 * @noinspection SpringJavaInjectionPointsAutowiringInspection
	 */
	@Autowired
	private Listener listener;

	@Test
	void testSendAndReceiveMessage() throws Exception {

		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);

		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(TOPIC);
		template.sendDefault("foo");

		template.sendDefault(0, 2, "bar");
		template.send(TOPIC, 0, 2, "baz");

		ConsumerRecordAssert.assertThat(listener.getRecords().poll(10, TimeUnit.SECONDS)).hasValue("foo");

		ConsumerRecord<Integer, String> received = listener.getRecords().poll(10, TimeUnit.SECONDS);
		ConsumerRecordAssert.assertThat(received).hasKey(2);
		ConsumerRecordAssert.assertThat(received).hasPartition(0);
		ConsumerRecordAssert.assertThat(received).hasValue("bar");

		received = listener.getRecords().poll(10, TimeUnit.SECONDS);
		ConsumerRecordAssert.assertThat(received).hasKey(2);
		ConsumerRecordAssert.assertThat(received).hasPartition(0);
		ConsumerRecordAssert.assertThat(received).hasValue("baz");
	}
}
