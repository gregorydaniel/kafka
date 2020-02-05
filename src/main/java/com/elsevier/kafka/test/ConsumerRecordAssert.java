package com.elsevier.kafka.test;

import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.AbstractAssert;

/**
 * AssertJ assertions for consumer record class.
 */
public class ConsumerRecordAssert extends AbstractAssert<ConsumerRecordAssert, ConsumerRecord> {
	public ConsumerRecordAssert(ConsumerRecord actual) {
		super(actual, ConsumerRecordAssert.class);
	}

	// fluent entry point
	public static ConsumerRecordAssert assertThat(ConsumerRecord actual) {
		return new ConsumerRecordAssert(actual);
	}

	public ConsumerRecordAssert hasKey(Object expectedKey) {
		isNotNull();

		if (!Objects.equals(actual.key(), expectedKey)) {
			failWithMessage("Expected key to be <%s> but was <%s>", expectedKey, actual.key());
		}

		return this;
	}

	public ConsumerRecordAssert hasValue(Object expectedValue) {
		isNotNull();

		if (!Objects.equals(actual.value(), expectedValue)) {
			failWithMessage("Expected value to be <%s> but was <%s>", expectedValue, actual.value());
		}

		return this;
	}

	public ConsumerRecordAssert hasPartition(Object expectedValue) {
		isNotNull();

		if (!Objects.equals(actual.partition(), expectedValue)) {
			failWithMessage("Expected partition to be <%s> but was <%s>", expectedValue, actual.partition());
		}

		return this;
	}
}