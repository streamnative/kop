package io.streamnative.kafka.client.api;

import java.lang.reflect.InvocationTargetException;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A completable class of org.apache.kafka.clients.consumer.OffsetAndMetadata.
 */
@AllArgsConstructor
@Getter
public class TopicOffsetAndMetadata {

    private String topic;
    private int partition;
    private long offset;

    public <T> T createTopicPartition(final Class<T> clazz) {
        try {
            return clazz.getConstructor(
                    String.class, int.class
            ).newInstance(topic, partition);
        } catch (InvocationTargetException
                | InstantiationException
                | IllegalAccessException
                | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public <T> T createOffsetAndMetadata(final Class<T> clazz) {
        try {
            return clazz.getConstructor(
                    long.class
            ).newInstance(offset);
        } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

    }
}
