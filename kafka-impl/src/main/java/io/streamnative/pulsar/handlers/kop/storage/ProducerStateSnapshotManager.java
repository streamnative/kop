package io.streamnative.pulsar.handlers.kop.storage;


import org.apache.pulsar.client.api.MessageId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ProducerStateSnapshotManager {

    CompletableFuture<MessageId> takeSnapshot(Map<Long, ProducerStateEntry> entries, long snapshotOffset);

    CompletableFuture<List<ProducerStateEntry>> loadSnapshots();
}
