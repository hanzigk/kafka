package org.apache.kafka.common.internals;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class TopicParttationTest {
    @Test
    public void shouldAcceptValidTopicNames() {
        String maxLengthString = TestUtils.randomString(249);
        String[] validTopicNames = {"valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_.", "...", maxLengthString};

        for (String topicName : validTopicNames) {
            Topic.validate(topicName);
        }
    }

    @Test
    public void shouldThrowOnInvalidTopicNames() {
        char[] longString = new char[250];
        Arrays.fill(longString, 'a');
        String[] invalidTopicNames = {"", "foo bar", "..", "foo:bar", "foo=bar", ".", new String(longString)};

        for (String topicName : invalidTopicNames) {
            try {
                Topic.validate(topicName);
                fail("No exception was thrown for topic with invalid name: " + topicName);
            } catch (InvalidTopicException e) {
                System.out.println(e);// Good
            }
        }
    }

    @Test
    public void testTopicHasCollision() {
        List<String> periodFirstMiddleLastNone = Arrays.asList(".topic", "to.pic", "topic.", "topic");
        List<String> underscoreFirstMiddleLastNone = Arrays.asList("_topic", "to_pic", "topic_", "topic");

        // Self
        for (String topic : periodFirstMiddleLastNone)
            assertTrue(Topic.hasCollision(topic, topic));

        for (String topic : underscoreFirstMiddleLastNone)
            assertTrue(Topic.hasCollision(topic, topic));

        // Same Position
        for (int i = 0; i < periodFirstMiddleLastNone.size(); ++i)
            assertTrue(Topic.hasCollision(periodFirstMiddleLastNone.get(i), underscoreFirstMiddleLastNone.get(i)));

        // Different Position
        Collections.reverse(underscoreFirstMiddleLastNone);
        for (int i = 0; i < periodFirstMiddleLastNone.size(); ++i)
            assertFalse(Topic.hasCollision(periodFirstMiddleLastNone.get(i), underscoreFirstMiddleLastNone.get(i)));
    }
}
