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

import static io.streamnative.pulsar.handlers.kop.KafkaRequestHandler.lookupDataContainsAddress;

import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;


/**
 * Kop broker lookup manager.
 */
@Slf4j
public class KopBrokerLookupManager {

    private final PulsarService pulsarService;
    private final String advertisedListeners;
    private final LookupClient lookupClient;
    private final MetadataCache<LocalBrokerData> localBrokerDataCache;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static final ConcurrentHashMap<String, ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>>
            LOOKUP_CACHE = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, CompletableFuture<Optional<String>>>
            KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    public KopBrokerLookupManager(PulsarService pulsarService, String advertisedListeners) {
        this.pulsarService = pulsarService;
        this.advertisedListeners = advertisedListeners;
        this.lookupClient = KafkaProtocolHandler.getLookupClient(pulsarService);
        this.localBrokerDataCache = pulsarService.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);
    }

    public CompletableFuture<Optional<InetSocketAddress>> findBroker(@NonNull TopicName topic,
                                                                     @Nullable EndPoint advertisedEndPoint) {
        if (log.isDebugEnabled()) {
            log.debug("Handle Lookup for topic {}", topic);
        }
        CompletableFuture<Optional<InetSocketAddress>> returnFuture = new CompletableFuture<>();

        getTopicBroker(topic.toString(),
                advertisedEndPoint != null && advertisedEndPoint.isMultiListener()
                        ? advertisedEndPoint.getListenerName() : null)
                .thenApply(address -> getProtocolDataToAdvertise(address, topic, advertisedEndPoint))
                .thenAccept(kopAddressFuture -> kopAddressFuture.thenAccept(listenersOptional -> {
                    if (!listenersOptional.isPresent()) {
                        log.error("Not get advertise data for Kafka topic:{}.", topic);
                        removeTopicManagerCache(topic.toString());
                        returnFuture.complete(Optional.empty());
                        return;
                    }

                    // It's the `kafkaAdvertisedListeners` config that's written to ZK
                    final String listeners = listenersOptional.get();
                    final EndPoint endPoint =
                            (advertisedEndPoint != null && advertisedEndPoint.isTlsEnabled()
                                    ? EndPoint.getSslEndPoint(listeners) : EndPoint.getPlainTextEndPoint(listeners));

                    if (log.isDebugEnabled()) {
                        log.debug("Found broker localListeners: {} for topicName: {}, "
                                        + "localListeners: {}, found Listeners: {}",
                                listeners, topic, advertisedListeners, listeners);
                    }

                    // here we found topic broker: broker2, but this is in broker1,
                    // how to clean the lookup cache?
                    if (!advertisedListeners.contains(endPoint.getOriginalListener())) {
                        removeTopicManagerCache(topic.toString());
                    }
                    returnFuture.complete(Optional.of(endPoint.getInetAddress()));
                })).exceptionally(throwable -> {
                    log.error("Not get advertise data for Kafka topic:{}. throwable: [{}]",
                            topic, throwable.getMessage());
                    removeTopicManagerCache(topic.toString());
                    returnFuture.complete(Optional.empty());
                    return null;
                });
        return returnFuture;
    }

    // call pulsarclient.lookup.getbroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName, String listenerName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("Return null for getTopicBroker({}) since channel closing", topicName);
            }
            return CompletableFuture.completedFuture(null);
        }

        ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>> topicLookupCache =
                LOOKUP_CACHE.computeIfAbsent(topicName, t-> {
                    if (log.isDebugEnabled()) {
                        log.debug("Topic {} not in Lookup_cache, call lookupBroker", topicName);
                    }
                    ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>> cache = new ConcurrentHashMap<>();
                    cache.put(listenerName == null ? "" : listenerName, lookupBroker(topicName, listenerName));
                    return cache;
                });

        return topicLookupCache.computeIfAbsent(listenerName == null ? "" : listenerName, t-> {
            if (log.isDebugEnabled()) {
                log.debug("Topic {} not in Lookup_cache, call lookupBroker", topicName);
            }
            return lookupBroker(topicName, listenerName);
        });
    }

    private CompletableFuture<InetSocketAddress> lookupBroker(final String topic, String listenerName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("Return null for getTopic({}) since channel closing", topic);
            }
            return CompletableFuture.completedFuture(null);
        }
        return lookupClient.getBrokerAddress(TopicName.get(topic), listenerName);
    }

    private CompletableFuture<Optional<String>> getProtocolDataToAdvertise(
            InetSocketAddress pulsarAddress, TopicName topic, @Nullable EndPoint advertisedEndPoint) {
        CompletableFuture<Optional<String>> returnFuture = new CompletableFuture<>();

        if (pulsarAddress == null) {
            log.error("[{}] failed get pulsar address, returned null.", topic.toString());

            // getTopicBroker returns null. topic should be removed from LookupCache.
            removeTopicManagerCache(topic.toString());

            returnFuture.complete(Optional.empty());
            return returnFuture;
        }

        if (log.isDebugEnabled()) {
            log.debug("Found broker for topic {} puslarAddress: {}",
                    topic, pulsarAddress);
        }

        // get kop address from cache to prevent query zk each time.
        final CompletableFuture<Optional<String>> future = KOP_ADDRESS_CACHE.get(topic.toString());
        if (future != null) {
            return future;
        }

        if (advertisedEndPoint != null && advertisedEndPoint.isMultiListener()) {
            // if kafkaProtocolMap is set, the lookup result is the advertised address
            String kafkaAdvertisedAddress = String.format("%s://%s:%s", advertisedEndPoint.getSecurityProtocol().name,
                    pulsarAddress.getHostName(), pulsarAddress.getPort());
            KOP_ADDRESS_CACHE.put(topic.toString(), returnFuture);
            returnFuture.complete(Optional.ofNullable(kafkaAdvertisedAddress));
            if (log.isDebugEnabled()) {
                log.debug("{} get kafka Advertised Address through kafkaListenerName: {}",
                        topic, pulsarAddress);
            }
            return returnFuture;
        }

        // advertised data is write in  /loadbalance/brokers/advertisedAddress:webServicePort
        // here we get the broker url, need to find related webServiceUrl.
        pulsarService.getPulsarResources()
                .getDynamicConfigResources()
                .getChildrenAsync(LoadManager.LOADBALANCE_BROKERS_ROOT)
                .whenComplete((set, throwable) -> {
                    if (throwable != null) {
                        log.error("Error in getChildrenAsync(zk://loadbalance) for {}", pulsarAddress, throwable);
                        returnFuture.complete(Optional.empty());
                        return;
                    }

                    String hostAndPort = pulsarAddress.getHostName() + ":" + pulsarAddress.getPort();
                    List<String> matchBrokers = Lists.newArrayList();
                    // match host part of url
                    for (String activeBroker : set) {
                        if (activeBroker.startsWith(pulsarAddress.getHostName() + ":")) {
                            matchBrokers.add(activeBroker);
                        }
                    }

                    if (matchBrokers.isEmpty()) {
                        log.error("No node for broker {} under zk://loadbalance", pulsarAddress);
                        returnFuture.complete(Optional.empty());
                        removeTopicManagerCache(topic.toString());
                        return;
                    }

                    // Get a list of ServiceLookupData for each matchBroker.
                    List<CompletableFuture<Optional<LocalBrokerData>>> list = matchBrokers.stream()
                            .map(matchBroker -> localBrokerDataCache.get(
                                    String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, matchBroker)))
                            .collect(Collectors.toList());

                    FutureUtil.waitForAll(list).whenComplete((ignore, th) -> {
                        if (th != null) {
                            log.error("Error in getDataAsync() for {}", pulsarAddress, th);
                            returnFuture.complete(Optional.empty());
                            removeTopicManagerCache(topic.toString());
                            return;
                        }

                        try {
                            for (CompletableFuture<Optional<LocalBrokerData>> lookupData : list) {
                                ServiceLookupData data = lookupData.get().get();
                                if (log.isDebugEnabled()) {
                                    log.debug("Handle getProtocolDataToAdvertise for {}, pulsarUrl: {}, "
                                                    + "pulsarUrlTls: {}, webUrl: {}, webUrlTls: {} kafka: {}",
                                            topic,
                                            data.getPulsarServiceUrl(),
                                            data.getPulsarServiceUrlTls(),
                                            data.getWebServiceUrl(),
                                            data.getWebServiceUrlTls(),
                                            data.getProtocol(KafkaProtocolHandler.PROTOCOL_NAME));
                                }

                                if (lookupDataContainsAddress(data, hostAndPort)) {
                                    KOP_ADDRESS_CACHE.put(topic.toString(), returnFuture);
                                    returnFuture.complete(data.getProtocol(KafkaProtocolHandler.PROTOCOL_NAME));
                                    return;
                                }
                            }
                        } catch (Exception e) {
                            log.error("Error in {} lookupFuture get: ", pulsarAddress, e);
                            returnFuture.complete(Optional.empty());
                            removeTopicManagerCache(topic.toString());
                            return;
                        }

                        // no matching lookup data in all matchBrokers.
                        log.error("Not able to search {} in all child of zk://loadbalance", pulsarAddress);
                        returnFuture.complete(Optional.empty());
                    });
                });
        return returnFuture;
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
    }

}
