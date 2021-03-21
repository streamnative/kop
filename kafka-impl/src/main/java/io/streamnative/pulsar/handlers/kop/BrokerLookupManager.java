package io.streamnative.pulsar.handlers.kop;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;

/**
 * Broker lookup manager.
 */
@Slf4j
public class BrokerLookupManager {

    private final PulsarService pulsarService;

    private final Map<String, CompletableFuture<InetSocketAddress>> cacheMap = new HashMap<>();

    public BrokerLookupManager(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
    }

    public CompletableFuture<InetSocketAddress> findBroker(String topicPartition) {
        return cacheMap.computeIfAbsent(topicPartition, key -> lookupBroker(topicPartition));
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    public CompletableFuture<InetSocketAddress> lookupBroker(String topicPartition) {
        CompletableFuture<InetSocketAddress> future = new CompletableFuture<>();
        try {
            ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                    .getBroker(TopicName.get(topicPartition))
                    .thenAccept(pair -> {
                        checkState(pair.getLeft().equals(pair.getRight()));
                        future.complete(pair.getLeft());
                    })
                    .exceptionally(th -> {
                        log.warn("[{}] getBroker for topic failed. throwable: ", topicPartition, th);
                        future.completeExceptionally(th);
                        return null;
                    });
        } catch (PulsarServerException e) {
            log.error("GetTopicBroker for topic {} failed get pulsar client, return null. throwable: ",
                    topicPartition, e);
            future.completeExceptionally(e);
        }
        return future;
    }

}
