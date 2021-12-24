package io.streamnative.pulsar.handlers.kop.storage;

import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import lombok.AllArgsConstructor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor
public class TopicProducerStateSnapshotManager implements ProducerStateSnapshotManager {

    private TopicName topicName;

    private final SystemTopicClient topicClient;


    @Override
    public CompletableFuture<MessageId> takeSnapshot(Map<Long, ProducerStateEntry> entries, long snapshotOffset) {
        return null;
    }

    @Override
    public CompletableFuture<List<ProducerStateEntry>> loadSnapshots() {
        return null;
    }
}
