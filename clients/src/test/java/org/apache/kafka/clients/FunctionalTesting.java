package org.apache.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.test.MockSerializer;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Duration;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FunctionalTesting {
    private final String topic = "topic";
    private MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private MockProducer<byte[], byte[]> producer;
    private final ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic, "key1".getBytes(), "value1".getBytes());
    private final ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(topic, "key2".getBytes(), "value2".getBytes());
    private void buildMockProducer(boolean autoComplete) {
        this.producer = new MockProducer<>(autoComplete, new MockSerializer(), new MockSerializer());
    }

    @Test
    public void ashouldInitTransactions() {
        buildMockProducer(true);
        producer.initTransactions();
        assertTrue(producer.transactionInitialized());
    }
    @Test
    public void bshouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions() {
        buildMockProducer(true);
        producer.initTransactions();
        try {
            producer.initTransactions();
            fail("Should have thrown as producer is already initialized");
        } catch (IllegalStateException e) { }
    }

    @Test(expected = IllegalStateException.class)
    public void chouldThrowOnBeginTransactionIfTransactionsNotInitialized() {
        buildMockProducer(true);
        producer.beginTransaction();
    }
    @Test
    public void dhouldBeginTransactions() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        assertTrue(producer.transactionInFlight());
    }
    @Test
    public void ehouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        producer.send(record1);
        producer.send(record2);

        assertTrue(producer.history().isEmpty());

        producer.commitTransaction();

        List<ProducerRecord<byte[], byte[]>> expectedResult = new ArrayList<>();
        expectedResult.add(record1);
        expectedResult.add(record2);

        assertThat(producer.history(), equalTo(expectedResult));
    }
    @Test
    public void ftestSimpleMockConsumer() {
        consumer.subscribe(Collections.singleton("test"));
        assertEquals(0, consumer.poll(Duration.ZERO).count());
        consumer.rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)));
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test", 0), 0L);
        beginningOffsets.put(new TopicPartition("test", 1), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
        consumer.seek(new TopicPartition("test", 0), 0);
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key1", "value1");
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key2", "value2");
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(1));
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        final TopicPartition tp = new TopicPartition("test", 0);
        assertEquals(2L, consumer.position(tp));
        consumer.commitSync();
        assertEquals(2L, consumer.committed(Collections.singleton(tp)).get(tp).offset());
    }
}
