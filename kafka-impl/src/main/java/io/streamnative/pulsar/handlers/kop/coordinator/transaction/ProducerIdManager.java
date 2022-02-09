package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import java.util.concurrent.CompletableFuture;

public interface ProducerIdManager {
    CompletableFuture<Void> initialize();

    CompletableFuture<Long> generateProducerId();

    void shutdown();
}
