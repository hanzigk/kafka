package org.apache.kafka.clients.consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.*;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ConsumerRecordsTestMock {
    @Test
    public void testQuery() {
        ConsumerRecord t = mock(ConsumerRecord.class);
        doReturn(t).when(t).makeRecord(anyString(),anyInt(),anyLong(),anyLong(),any(TimestampType.class),anyLong(),anyInt(),anyInt(),any(),any(),any(Headers.class),any());
        when(t.topic()).thenReturn("topic");
        when(t.partition()).thenReturn(1);
        when(t.offset()).thenReturn((long) 0).thenReturn((long) 1).thenReturn((long) 2);
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();
        String topic = "topic";
        records.put(new TopicPartition(topic, 0), new ArrayList<ConsumerRecord<Integer, String>>());
        ConsumerRecord<Integer, String> record1 = new ConsumerRecord<>(topic, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1, "value1");
        ConsumerRecord<Integer, String> record2 = new ConsumerRecord<>(topic, 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2, "value2");
        records.put(new TopicPartition(topic, 1), Arrays.asList(record1, record2));
        records.put(new TopicPartition(topic, 2), new ArrayList<ConsumerRecord<Integer, String>>());
        for(int i = 0; i <3; i ++)
        {
            assertEquals("topic",t.topic());
            assertEquals(1,t.partition());
            assertEquals(i,t.offset());
        }

    }


}
