package io.streamnative.pulsar.handlers.kop.storage;

import lombok.AllArgsConstructor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor
public class TopicProducerStateSnapshotManager implements ProducerStateSnapshotManager {

    private static TopicName topicName;



    @Override
    public CompletableFuture<MessageId> takeSnapshot(Map<Long, ProducerStateEntry> entries, long snapshotOffset) {
        return null;
    }

    @Override
    public CompletableFuture<List<ProducerStateEntry>> loadSnapshots() {
        return null;
    }
}
