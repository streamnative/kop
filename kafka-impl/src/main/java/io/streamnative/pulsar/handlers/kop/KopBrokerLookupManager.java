/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.MetadataStoreCacheLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;


/**
 * Kop broker lookup manager.
 */
@Slf4j
public class KopBrokerLookupManager {

    private final LookupClient lookupClient;
    private final MetadataStoreCacheLoader metadataStoreCacheLoader;
    private final String selfAdvertisedListeners;

    private final PulsarService pulsar;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
            LOOKUP_CACHE = new ConcurrentHashMap<>();

    public KopBrokerLookupManager(KafkaServiceConfiguration conf, PulsarService pulsarService,
                                  LookupClient lookupClient) throws Exception {
        this.pulsar = pulsarService;
        this.lookupClient = lookupClient;
        this.metadataStoreCacheLoader = new MetadataStoreCacheLoader(pulsarService.getPulsarResources(),
                conf.getBrokerLookupTimeoutMs());
        this.selfAdvertisedListeners = conf.getKafkaAdvertisedListeners();
    }

    public CompletableFuture<Optional<InetSocketAddress>> findBroker(String topic,
                                                                     @Nullable EndPoint advertisedEndPoint) {
        return getTopicBroker(topic)
                .thenApply(internalListenerAddress -> {
                    if (internalListenerAddress == null) {
                        log.error("[{}] failed get pulsar address, returned null.", topic);
                        removeTopicManagerCache(topic);
                        return Optional.empty();
                    } else if (log.isDebugEnabled()) {
                        log.debug("[{}] Found broker's internal listener address: {}",
                                topic, internalListenerAddress);
                    }

                    try {
                        final String listener = getAdvertisedListener(
                                internalListenerAddress, topic, advertisedEndPoint);
                        if (listener == null) {
                            log.error("Failed to find the advertised listener for {} ", topic);
                            removeTopicManagerCache(topic);
                            return Optional.empty();
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Found listener {} for topic {}", listener, topic);
                        }
                        final AdvertisedListener advertisedListener = AdvertisedListener.create(listener);

                        return Optional.of(new InetSocketAddress(
                                advertisedListener.getHostname(),
                                advertisedListener.getPort()));
                    } catch (IllegalStateException | NumberFormatException e) {
                        log.error("Failed to find the advertised listener: {}", e.getMessage());
                        removeTopicManagerCache(topic);
                        return Optional.empty();
                    }
                });
    }

    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("Return null for getTopicBroker({}) since channel closing", topicName);
            }
            return CompletableFuture.completedFuture(null);
        }

        if (log.isDebugEnabled()) {
            log.debug("Handle Lookup for topic {}", topicName);
        }

        final CompletableFuture<InetSocketAddress> future = LOOKUP_CACHE.get(topicName);
        return (future != null) ? future : lookupBroker(topicName);
    }

    private CompletableFuture<InetSocketAddress> lookupBroker(final String topic) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("Return null for getTopic({}) since channel closing", topic);
            }
            return CompletableFuture.completedFuture(null);
        }
        return lookupClient.getBrokerAddress(TopicName.get(topic));
    }

    public CompletableFuture<Boolean> isTopicExists(final String topic) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        TopicName topicName = TopicName.get(topic);
        this.pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(TopicName.get(topic))
                .whenComplete((metadata, ex) -> {
                    if (ex != null) {
                        log.error("Fetch partitioned topic metadata has exception.", ex);
                        future.complete(true);
                        return;
                    }
                    if (metadata.partitions == 0) {
                        internalCheckTopicExists(topicName).thenAccept(future::complete)
                                .exceptionally(throwable -> {
                                    log.error("Check topic exists has exception.", throwable);
                                    future.complete(true);
                                    return null;
                                });
                        return;
                    }
                    future.complete(true);
                });
        return future;
    }

    protected CompletableFuture<Boolean> internalCheckTopicExists(TopicName topicName) {
        return this.pulsar.getNamespaceService().checkTopicExists(topicName);
    }


    private String getAdvertisedListener(InetSocketAddress internalListenerAddress,
                                         String topic,
                                         @Nullable EndPoint advertisedEndPoint) {

        final List<LoadManagerReport> availableBrokers = metadataStoreCacheLoader.getAvailableBrokers();
        if (log.isDebugEnabled()) {
            availableBrokers.forEach(loadManagerReport ->
                    log.debug("Handle getProtocolDataToAdvertise for {}, pulsarUrl: {}, "
                                    + "pulsarUrlTls: {}, webUrl: {}, webUrlTls: {} kafka: {}",
                            topic,
                            loadManagerReport.getPulsarServiceUrl(),
                            loadManagerReport.getPulsarServiceUrlTls(),
                            loadManagerReport.getWebServiceUrl(),
                            loadManagerReport.getWebServiceUrlTls(),
                            loadManagerReport.getProtocol(KafkaProtocolHandler.PROTOCOL_NAME)));
        }

        final String hostAndPort = internalListenerAddress.getHostName() + ":" + internalListenerAddress.getPort();
        final Optional<LoadManagerReport> serviceLookupData = availableBrokers.stream()
                .filter(loadManagerReport -> lookupDataContainsAddress(loadManagerReport, hostAndPort)).findAny();
        if (!serviceLookupData.isPresent()) {
            log.error("No node for broker {} under loadBalance", internalListenerAddress);
            return null;
        }

        return serviceLookupData.get().getProtocol(KafkaProtocolHandler.PROTOCOL_NAME).map(kafkaAdvertisedListeners -> {
            if (kafkaAdvertisedListeners.equals(selfAdvertisedListeners)) {
                // the topic is owned by this broker, cache the look up result
                LOOKUP_CACHE.put(topic, CompletableFuture.completedFuture(internalListenerAddress));
            }
            return Optional.ofNullable(advertisedEndPoint)
                    .map(endPoint -> EndPoint.findListener(kafkaAdvertisedListeners, endPoint.getListenerName()))
                    .orElse(EndPoint.findFirstListener(kafkaAdvertisedListeners));
        }).orElseThrow(() -> new IllegalStateException(
                "No kafkaAdvertisedListeners found in broker " + internalListenerAddress));
    }

    // whether a ServiceLookupData contains wanted address.
    private static boolean lookupDataContainsAddress(ServiceLookupData data, String hostAndPort) {
        return StringUtils.endsWith(data.getPulsarServiceUrl(), hostAndPort)
                || StringUtils.endsWith(data.getPulsarServiceUrlTls(), hostAndPort);
    }

    public static void removeTopicManagerCache(String topicName) {
        LOOKUP_CACHE.remove(topicName);
    }

    public static void clear() {
        LOOKUP_CACHE.clear();
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            if (log.isDebugEnabled()) {
                log.debug("Closing KopBrokerLookupManager");
            }
            return;
        }
        clear();
        try {
            metadataStoreCacheLoader.close();
        } catch (IOException e) {
            log.error("Close metadataStoreCacheLoader failed.", e);
        }
    }
}
