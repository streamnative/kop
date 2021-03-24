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

import static com.google.common.base.Preconditions.checkState;
import static io.streamnative.pulsar.handlers.kop.KafkaRequestHandler.lookupDataContainsAddress;

import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.ZooKeeperCache;


/**
 * Broker lookup manager.
 */
@Slf4j
public class KoPBrokerLookupManager {

    private final PulsarService pulsarService;
    private final Boolean tlsEnabled;
    private final String advertisedListeners;

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
            LOOKUP_CACHE = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, CompletableFuture<Optional<String>>>
            KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    private final Map<String, CompletableFuture<InetSocketAddress>> cacheMap = new HashMap<>();

    public KoPBrokerLookupManager(PulsarService pulsarService, Boolean tlsEnabled, String advertisedListeners) {
        this.pulsarService = pulsarService;
        this.tlsEnabled = tlsEnabled;
        this.advertisedListeners = advertisedListeners;
    }

    public CompletableFuture<InetSocketAddress> findBroker(String topicPartition) {
        return cacheMap.computeIfAbsent(topicPartition, key -> lookup(TopicName.get(topicPartition)));
    }

    public CompletableFuture<InetSocketAddress> lookup(TopicName topic) {
        if (log.isDebugEnabled()) {
            log.debug("Handle Lookup for {}", topic);
        }
        CompletableFuture<InetSocketAddress> returnFuture = new CompletableFuture<>();

        getTopicBroker(topic.toString())
                .thenCompose(pair -> getProtocolDataToAdvertise(pair, topic))
                .whenComplete((stringOptional, throwable) -> {
                    if (!stringOptional.isPresent() || throwable != null) {
                        log.error("Not get advertise data for Kafka topic:{}. throwable", topic, throwable);
                        returnFuture.complete(null);
                        return;
                    }

                    // It's the `kafkaAdvertisedListeners` config that's written to ZK
                    final String listeners = stringOptional.get();
                    final EndPoint endPoint = tlsEnabled
                            ? EndPoint.getSslEndPoint(listeners) : EndPoint.getPlainTextEndPoint(listeners);

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

                    if (!advertisedListeners.contains(endPoint.getOriginalListener())) {
                        returnFuture.complete(endPoint.getInetAddress());
                        return;
                    }

                    getTopicBroker(topic.toString()).whenComplete((persistentTopic, exception) -> {
                        if (exception != null || persistentTopic == null) {
                            log.warn("findBroker: Failed to getOrCreateTopic {}. broker:{}, exception:",
                                    topic.toString(), endPoint.getOriginalListener(), exception);
                            // remove cache when topic is null
                            removeTopicManagerCache(topic.toString());
                            returnFuture.complete(null);
                            return;
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("Add topic: {} into TopicManager while findBroker.", topic.toString());
                        }
                        returnFuture.complete(endPoint.getInetAddress());
                    });
                });
        return returnFuture;
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicPartition) {
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

    private CompletableFuture<Optional<String>> getProtocolDataToAdvertise(
            InetSocketAddress pulsarAddress, TopicName topic) {

        CompletableFuture<Optional<String>> returnFuture = new CompletableFuture<>();

        if (pulsarAddress == null) {
            log.error("[{}] failed get pulsar address, returned null.", topic.toString());

            // getTopicBroker returns null. topic should be removed from LookupCache.
            removeTopicManagerCache(topic.toString());

            returnFuture.complete(Optional.empty());
            return returnFuture;
        }

        if (log.isDebugEnabled()) {
            log.debug("Found broker for topic {} puslarAddress: {}", topic, pulsarAddress);
        }

        // get kop address from cache to prevent query zk each time.
        if (KOP_ADDRESS_CACHE.containsKey(topic.toString())) {
            return KOP_ADDRESS_CACHE.get(topic.toString());
        }
        // advertised data is write in  /loadbalance/brokers/advertisedAddress:webServicePort
        // here we get the broker url, need to find related webServiceUrl.
        ZooKeeperCache zkCache = pulsarService.getLocalZkCache();
        zkCache.getChildrenAsync(LoadManager.LOADBALANCE_BROKERS_ROOT, zkCache)
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
                    List<CompletableFuture<Optional<ServiceLookupData>>> list = matchBrokers.stream()
                            .map(matchBroker ->
                                zkCache.getDataAsync(
                                    String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, matchBroker),
                                    (ZooKeeperCache.Deserializer<ServiceLookupData>)
                                            pulsarService.getLoadManager().get().getLoadReportDeserializer()))
                            .collect(Collectors.toList());

                    FutureUtil.waitForAll(list)
                        .whenComplete((ignore, th) -> {
                            if (th != null) {
                                log.error("Error in getDataAsync() for {}", pulsarAddress, th);
                                returnFuture.complete(Optional.empty());
                                removeTopicManagerCache(topic.toString());
                                return;
                            }

                            try {
                                for (CompletableFuture<Optional<ServiceLookupData>> lookupData : list) {
                                    ServiceLookupData data = lookupData.get().get();
                                    if (log.isDebugEnabled()) {
                                        log.debug("Handle getProtocolDataToAdvertise for {}, pulsarUrl: {}, "
                                                        + "pulsarUrlTls: {}, webUrl: {}, webUrlTls: {} kafka: {}",
                                                topic, data.getPulsarServiceUrl(), data.getPulsarServiceUrlTls(),
                                                data.getWebServiceUrl(), data.getWebServiceUrlTls(),
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

}
