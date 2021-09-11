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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.eclipse.jetty.util.StringUtil;


/**
 * Broker lookup manager.
 */
@Slf4j
public class KopBrokerLookupManager {

    private final PulsarService pulsarService;
    private final Boolean tlsEnabled;
    private final String advertisedListeners;
    private final LookupClient lookupClient;

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
            LOOKUP_CACHE = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, CompletableFuture<Optional<String>>>
            KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    public KopBrokerLookupManager(PulsarService pulsarService, Boolean tlsEnabled, String advertisedListeners) {
        this.pulsarService = pulsarService;
        this.tlsEnabled = tlsEnabled;
        this.advertisedListeners = advertisedListeners;
        this.lookupClient = KafkaProtocolHandler.getLookupClient(pulsarService);
    }

    public CompletableFuture<InetSocketAddress> findBroker(String topic) {
        if (log.isDebugEnabled()) {
            log.debug("Handle Lookup for topic {}", topic);
        }

        CompletableFuture<InetSocketAddress> returnFuture = new CompletableFuture<>();
        getTopicBroker(topic)
                .thenApply(address -> getProtocolDataToAdvertise(address, TopicName.get(topic)))
                .thenAccept(kopAddressFuture -> kopAddressFuture.thenAccept(listeners -> {
                    if (!listeners.isPresent()) {
                        log.error("Not get advertise data for Kafka topic:{}", topic);
                        removeTopicManagerCache(topic);
                        returnFuture.complete(null);
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Found broker localListeners: {} for topicName: {}, localListeners: {}",
                                listeners.get(), topic, advertisedListeners);
                    }

                    // It's the `kafkaAdvertisedListeners` config that's written to ZK
                    final EndPoint endPoint =
                            tlsEnabled ? EndPoint.getSslEndPoint(listeners.get())
                                    : EndPoint.getPlainTextEndPoint(listeners.get());

                    // here we found topic broker: broker2, but this is in broker1,
                    // how to clean the lookup cache?
                    if (!advertisedListeners.contains(endPoint.getOriginalListener())) {
                        removeTopicManagerCache(topic);
                        returnFuture.complete(endPoint.getInetAddress());
                        return;
                    }

                    checkTopicOwner(returnFuture, topic, endPoint);
                })).exceptionally(throwable -> {
                    log.error("Not get advertise data for Kafka topic:{}. throwable: [{}]",
                            topic, throwable.getMessage());
                    removeTopicManagerCache(topic);
                    returnFuture.complete(null);
                    return null;
                });
        return returnFuture;
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    private CompletableFuture<InetSocketAddress> getTopicBroker(String topicPartition) {
        return LOOKUP_CACHE.computeIfAbsent(topicPartition,
                ignored -> lookupClient.getBrokerAddress(TopicName.get(topicPartition)));
    }

    private void checkTopicOwner(CompletableFuture<InetSocketAddress> future, String topic, EndPoint endPoint) {
        getTopic(topic).whenComplete((persistentTopic, exception) -> {
            if (exception != null || persistentTopic == null) {
                log.warn("findBroker: Failed to getOrCreateTopic {}. broker:{}, exception:",
                        topic, endPoint.getOriginalListener(), exception);
                // remove cache when topic is null
                removeTopicManagerCache(topic);
                future.complete(null);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Add topic: {} into TopicManager while findBroker.", topic);
            }
            future.complete(endPoint.getInetAddress());
        });
    }

    private CompletableFuture<PersistentTopic> getTopic(String topicName) {
        CompletableFuture<PersistentTopic> topicCompletableFuture = new CompletableFuture<>();
        pulsarService.getBrokerService()
                .getTopic(topicName, pulsarService.getBrokerService().isAllowAutoTopicCreation(topicName))
                .whenComplete((topic, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to getTopic {}. exception:", topicName, throwable);
                        // failed to getTopic from current broker, remove cache, which added in getTopicBroker.
                        removeTopicManagerCache(topicName);
                        topicCompletableFuture.complete(null);
                        return;
                    }
                    if (topic.isPresent()) {
                        topicCompletableFuture.complete((PersistentTopic) topic.get());
                    } else {
                        log.error("Get empty topic for name {}", topicName);
                        topicCompletableFuture.complete(null);
                    }
                });
        return topicCompletableFuture;
    }

    private CompletableFuture<Optional<String>> getProtocolDataToAdvertise(
            InetSocketAddress pulsarAddress, TopicName topic) {

        CompletableFuture<Optional<String>> kopAddressFuture = new CompletableFuture<>();

        if (pulsarAddress == null) {
            log.error("[{}] failed get pulsar address, returned null.", topic.toString());

            // getTopicBroker returns null. topic should be removed from LookupCache.
            removeTopicManagerCache(topic.toString());

            kopAddressFuture.complete(Optional.empty());
            return kopAddressFuture;
        }

        if (log.isDebugEnabled()) {
            log.debug("Find broker for topic {} pulsarAddress: {}", topic, pulsarAddress);
        }

        // get kop address from cache to prevent query zk each time.
        if (KOP_ADDRESS_CACHE.containsKey(topic.toString())) {
            return KOP_ADDRESS_CACHE.get(topic.toString());
        }

        // if kafkaListenerName is set, the lookup result is the advertised address
        if (!StringUtil.isBlank(lookupClient.getPulsarClient().getConfiguration().getListenerName())) {
            // TODO:　should add SecurityProtocol according to which endpoint is handling the request.
            //  firstly we only support PLAINTEXT when lookup with kafkaListenerName
            String kafkaAdvertisedAddress = String.format("%s://%s:%s", SecurityProtocol.PLAINTEXT.name(),
                    pulsarAddress.getHostName(), pulsarAddress.getPort());
            KOP_ADDRESS_CACHE.put(topic.toString(), kopAddressFuture);
            if (log.isDebugEnabled()) {
                log.debug("{} get kafka Advertised Address through kafkaListenerName: {}",
                        topic, pulsarAddress);
            }
            kopAddressFuture.complete(Optional.ofNullable(kafkaAdvertisedAddress));
            return kopAddressFuture;
        }

        // advertised data is write in  /loadbalance/brokers/advertisedAddress:webServicePort
        // here we get the broker url, need to find related webServiceUrl.
        ZooKeeperCache zkCache = pulsarService.getLocalZkCache();
        zkCache.getChildrenAsync(LoadManager.LOADBALANCE_BROKERS_ROOT, zkCache)
                .whenComplete((set, throwable) -> {
                    if (throwable != null) {
                        log.error("Error in getChildrenAsync(zk://loadbalance) for {}", pulsarAddress, throwable);
                        kopAddressFuture.complete(Optional.empty());
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
                        kopAddressFuture.complete(Optional.empty());
                        removeTopicManagerCache(topic.toString());
                        return;
                    }

                    // Get a list of ServiceLookupData for each matchBroker.
                    final MetadataCache<LocalBrokerData> metadataCache = pulsarService.getLocalMetadataStore()
                            .getMetadataCache(LocalBrokerData.class);
                    List<CompletableFuture<Optional<LocalBrokerData>>> list = matchBrokers.stream()
                            .map(matchBroker -> metadataCache.get(
                                    String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, matchBroker)))
                            .collect(Collectors.toList());

                    getKopAddress(list, pulsarAddress, kopAddressFuture, topic, hostAndPort);
                });
        return kopAddressFuture;
    }

    private void getKopAddress(List<CompletableFuture<Optional<LocalBrokerData>>> list,
                               InetSocketAddress pulsarAddress,
                               CompletableFuture<Optional<String>> kopAddressFuture,
                               TopicName topic,
                               String hostAndPort) {
        FutureUtil.waitForAll(list)
                .whenComplete((ignore, th) -> {
                    if (th != null) {
                        log.error("Error in getDataAsync() for {}", pulsarAddress, th);
                        kopAddressFuture.complete(Optional.empty());
                        removeTopicManagerCache(topic.toString());
                        return;
                    }

                    try {
                        for (CompletableFuture<Optional<LocalBrokerData>> lookupData : list) {
                            ServiceLookupData data = lookupData.get().get();
                            if (log.isDebugEnabled()) {
                                log.debug("Handle getProtocolDataToAdvertise for {}, pulsarUrl: {}, "
                                                + "pulsarUrlTls: {}, webUrl: {}, webUrlTls: {} kafka: {}",
                                        topic, data.getPulsarServiceUrl(), data.getPulsarServiceUrlTls(),
                                        data.getWebServiceUrl(), data.getWebServiceUrlTls(),
                                        data.getProtocol(KafkaProtocolHandler.PROTOCOL_NAME));
                            }

                            if (lookupDataContainsAddress(data, hostAndPort)) {
                                KOP_ADDRESS_CACHE.put(topic.toString(), kopAddressFuture);
                                kopAddressFuture.complete(data.getProtocol(KafkaProtocolHandler.PROTOCOL_NAME));
                                return;
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error in {} lookupFuture get: ", pulsarAddress, e);
                        kopAddressFuture.complete(Optional.empty());
                        removeTopicManagerCache(topic.toString());
                        return;
                    }

                    // no matching lookup data in all matchBrokers.
                    log.error("Not able to search {} in all child of zk://loadbalance", pulsarAddress);
                    kopAddressFuture.complete(Optional.empty());
                });
    }

    public static void removeTopicManagerCache(String topicName) {
        LOOKUP_CACHE.remove(topicName);
        KOP_ADDRESS_CACHE.remove(topicName);
    }

    public static void clear() {
        LOOKUP_CACHE.clear();
        KOP_ADDRESS_CACHE.clear();
    }

}
