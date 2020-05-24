package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaConsumerNewTest {

    @Test
    public void TestConstructor()
    {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my.consumer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group.id");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,"100");
        props.setProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,"100");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaConsumerNew<byte[], byte[]> newConsumer = new KafkaConsumerNew<byte[], byte[]>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertEquals("my.consumer",newConsumer.getClientId());
        assertEquals("Optional[group.id]",newConsumer.getGroupId());
        assertEquals("100",newConsumer.getRequestTimeoutMs());
        assertEquals("100",newConsumer.getDefaultApiTimeoutMs());
    }

}
