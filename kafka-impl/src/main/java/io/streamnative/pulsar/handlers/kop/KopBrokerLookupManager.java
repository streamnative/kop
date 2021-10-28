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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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
    private final Map<String, SecurityProtocol> protocolMap;
    private final MetadataStoreCacheLoader metadataStoreCacheLoader;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
            LOOKUP_CACHE = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, String> KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    public KopBrokerLookupManager(KafkaServiceConfiguration conf, PulsarService pulsarService) throws Exception {
        this.lookupClient = KafkaProtocolHandler.getLookupClient(pulsarService);
        // TODO: change it to getKafkaAdvertisedProtocolMap
        this.protocolMap = EndPoint.parseProtocolMap(conf.getKafkaProtocolMap());
        this.metadataStoreCacheLoader = new MetadataStoreCacheLoader(pulsarService.getPulsarResources(),
                conf.getBrokerLookupTimeoutMs());
    }

    public CompletableFuture<Optional<InetSocketAddress>> findBroker(@NonNull TopicName topic,
                                                                     @Nullable EndPoint advertisedEndPoint) {
        if (log.isDebugEnabled()) {
            log.debug("Handle Lookup for topic {}", topic);
        }

        return getTopicBroker(topic.toString())
                .thenApply(internalListenerAddress -> {
                    final String listener = getAdvertisedListener(
                            internalListenerAddress, topic, advertisedEndPoint);
                    if (listener == null) {
                        removeTopicManagerCache(topic.toString());
                        return Optional.empty();
                    }

                    try {
                        final EndPoint endPoint = new EndPoint(listener, protocolMap);
                        return Optional.of(new InetSocketAddress(endPoint.getHostname(), endPoint.getPort()));
                    } catch (IllegalStateException e) {
                        log.error("Failed to create EndPoint from '{}': {}", listener, e.getMessage());
                        return Optional.empty();
                    }
                });
    }

    // call pulsarclient.lookup.getbroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("Return null for getTopicBroker({}) since channel closing", topicName);
            }
            return CompletableFuture.completedFuture(null);
        }

        return LOOKUP_CACHE.computeIfAbsent(topicName, this::lookupBroker);
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

    private String getAdvertisedListener(InetSocketAddress internalListenerAddress,
                                         TopicName topic,
                                         @Nullable EndPoint advertisedEndPoint) {
        if (internalListenerAddress == null) {
            log.error("[{}] failed get pulsar address, returned null.", topic);
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Found broker's internal listener address: {}",
                    topic, internalListenerAddress);
        }

        final String protocolDataToAdvertise = KOP_ADDRESS_CACHE.get(topic.toString());
        if (protocolDataToAdvertise != null) {
            return protocolDataToAdvertise;
        } // else: either the key doesn't exist or a null value was put

        List<LoadManagerReport> availableBrokers = metadataStoreCacheLoader.getAvailableBrokers();
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

        final String kafkaAdvertisedListeners =
                serviceLookupData.get().getProtocol(KafkaProtocolHandler.PROTOCOL_NAME).orElse(null);
        if (kafkaAdvertisedListeners == null) {
            log.error("No kafkaAdvertisedListeners found in broker {}", internalListenerAddress);
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Found kafkaAdvertisedListeners: {}", kafkaAdvertisedListeners);
        }

        final String listenerName = (advertisedEndPoint != null) ? advertisedEndPoint.getListenerName() : null;
        if (listenerName == null) {
            // Treat the whole kafkaAdvertisedListeners as the listener
            return kafkaAdvertisedListeners;
        } else {
            return EndPoint.findListener(kafkaAdvertisedListeners, listenerName);
        }
    }

    // whether a ServiceLookupData contains wanted address.
    private static boolean lookupDataContainsAddress(ServiceLookupData data, String hostAndPort) {
        return StringUtils.endsWith(data.getPulsarServiceUrl(), hostAndPort)
                || StringUtils.endsWith(data.getPulsarServiceUrlTls(), hostAndPort);
    }

    public static void removeTopicManagerCache(String topicName) {
        LOOKUP_CACHE.remove(topicName);
        KOP_ADDRESS_CACHE.remove(topicName);
    }

    public static void clear() {
        LOOKUP_CACHE.clear();
        KOP_ADDRESS_CACHE.clear();
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
