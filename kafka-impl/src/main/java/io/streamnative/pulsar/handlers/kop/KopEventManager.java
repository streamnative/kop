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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import io.streamnative.pulsar.handlers.kop.utils.ShutdownableThread;
import io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;

@Slf4j
public class KopEventManager {
    private static final String kopEventThreadName = "kop-event-thread";
    private static final LinkedBlockingQueue<KopEventWrapper> queue =
            new LinkedBlockingQueue<>();
    private final KopEventThread thread =
            new KopEventThread(kopEventThreadName);
    private final Map<String, GroupCoordinator> groupCoordinatorsByTenant;
    private final AdminManager adminManager;
    private final KafkaServiceConfiguration kafkaConfig;
    private final BrokersChangeHandler brokersChangeHandler;
    private final MetadataStore metadataStore;
    private KopEventManagerStats eventManagerStats;
    public BiConsumer<String, Long> registerEventLatency = (eventName, createdTime) -> {
        this.eventManagerStats.getStatsLogger()
                .scopeLabel(KopServerStats.KOP_EVENT_SCOPE, eventName)
                .getOpStatsLogger(KopServerStats.KOP_EVENT_LATENCY)
                .registerSuccessfulEvent(MathUtils.elapsedNanos(createdTime),
                        TimeUnit.NANOSECONDS);
    };

    public KopEventManager(AdminManager adminManager,
                           MetadataStore metadataStore,
                           StatsLogger statsLogger,
                           KafkaServiceConfiguration kafkaConfig,
                           Map<String, GroupCoordinator> groupCoordinatorsByTenant) {
        this.adminManager = adminManager;
        this.brokersChangeHandler = new BrokersChangeHandler(this);
        this.metadataStore = metadataStore;
        this.kafkaConfig = kafkaConfig;
        this.eventManagerStats = new KopEventManagerStats(statsLogger, queue);
        this.groupCoordinatorsByTenant = groupCoordinatorsByTenant;
    }

    public void start() {
        registerChildChangeHandler();
        thread.start();
    }

    public void close() {
        try {
            thread.initiateShutdown();
            clearAndPut(getShutdownEventThread());
            thread.awaitShutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted at shutting down {}", kopEventThreadName);
        }

    }


    public void put(KopEventWrapper eventWrapper) {
        try {
            queue.put(eventWrapper);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Error put event {} to kop event queue",
                    eventWrapper.toString(), e);
        }
    }

    public void clearAndPut(KopEventWrapper eventWrapper) {
        queue.clear();
        put(eventWrapper);
    }

    public void registerEventQueuedLatency(KopEventWrapper eventWrapper) {
        this.eventManagerStats.getStatsLogger()
                .scopeLabel(KopServerStats.KOP_EVENT_SCOPE, eventWrapper.kopEvent.name())
                .getOpStatsLogger(KopServerStats.KOP_EVENT_QUEUED_LATENCY)
                .registerSuccessfulEvent(MathUtils.elapsedNanos(eventWrapper.getCreatedTime()),
                        TimeUnit.NANOSECONDS);
    }

    class KopEventThread extends ShutdownableThread {

        public KopEventThread(String name) {
            super(name);
        }

        @Override
        protected void doWork() {
            KopEventWrapper eventWrapper = null;
            try {
                eventWrapper = queue.take();
                registerEventQueuedLatency(eventWrapper);

                if (eventWrapper.kopEvent instanceof ShutdownEventThread) {
                    log.info("Shutting down KopEventThread.");
                } else {
                    eventWrapper.kopEvent.process(registerEventLatency, MathUtils.nowInNano());
                }
            } catch (InterruptedException e) {
                log.error("Error processing event {}", eventWrapper, e);
            }
        }

    }

    private void registerChildChangeHandler() {
        metadataStore.registerListener(this::handleChildChangePathNotification);

        // init local kop brokers cache
        getBrokers(metadataStore.getChildren(getBrokersChangePath()).join(),
                null, "", -1);
    }

    private void handleChildChangePathNotification(Notification notification) {
        if (notification.getPath().equals(LoadManager.LOADBALANCE_BROKERS_ROOT)) {
            this.brokersChangeHandler.handleChildChange();
        }
    }

    private void getBrokers(List<String> pulsarBrokers,
                            BiConsumer<String, Long> registerEventLatency,
                            String name,
                            long startProcessTime) {
        ConcurrentMap<String, Set<Node>> kopBrokersMap = Maps.newConcurrentMap();
        final AtomicInteger pendingBrokers = new AtomicInteger(pulsarBrokers.size());

        pulsarBrokers.forEach(broker -> {
            metadataStore.get(getBrokersChangePath() + "/" + broker).whenComplete(
                    (brokerData, e) -> {
                        if (e != null) {
                            if (registerEventLatency != null) {
                                registerEventLatency.accept(name, startProcessTime);
                            }
                            log.error("Get broker {} path data failed which have an error", broker, e);
                            return;
                        }

                        if (brokerData.isPresent()) {
                            JsonObject jsonObject = parseJsonObject(
                                    new String(brokerData.get().getValue(), StandardCharsets.UTF_8));
                            JsonObject protocols = jsonObject.getAsJsonObject("protocols");
                            JsonElement element = protocols.get("kafka");

                            if (element != null) {
                                String kopBrokerStrs = element.getAsString();
                                Map<String, Set<Node>> kopNodesMap = getNodes(kopBrokerStrs);
                                kopNodesMap.forEach((listenerName, nodesSet) -> {
                                    Set<Node> currentNodeSet = kopBrokersMap.computeIfAbsent(listenerName,
                                            s -> Sets.newConcurrentHashSet());
                                    currentNodeSet.addAll(nodesSet);
                                    kopBrokersMap.put(listenerName, currentNodeSet);
                                });
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("Get broker {} path currently not a kop broker, skip it.", broker);
                                }
                            }
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Get broker {} path data empty.", broker);
                            }
                        }

                        if (pendingBrokers.decrementAndGet() == 0) {
                            Map<String, Set<Node>> oldKopBrokers = adminManager.getAllBrokers();
                            adminManager.setBrokers(kopBrokersMap);
                            if (registerEventLatency != null) {
                                registerEventLatency.accept(name, startProcessTime);
                            }
                            log.info("Refresh kop brokers new cache {}, old brokers cache {}",
                                    adminManager.getAllBrokers(), oldKopBrokers);
                        }
                    }
            );
        });

    }

    private JsonObject parseJsonObject(String info) {
        JsonParser parser = new JsonParser();
        return parser.parse(info).getAsJsonObject();
    }

    @VisibleForTesting
    public static Map<String, Set<Node>> getNodes(String kopBrokerStrs) {
        HashMap<String, Set<Node>> nodesMap = Maps.newHashMap();
        String[] kopBrokerArr = kopBrokerStrs.split(EndPoint.END_POINT_SEPARATOR);
        for (String kopBrokerStr : kopBrokerArr) {
            final String errorMessage = "kopBrokerStr " + kopBrokerStr + " is invalid";
            final Matcher matcher = EndPoint.matcherListener(kopBrokerStr, errorMessage);
            String listenerName = matcher.group(1);
            String host = matcher.group(2);
            String port = matcher.group(3);
            Set<Node> nodeSet = nodesMap.computeIfAbsent(listenerName, s -> new HashSet<>());
            nodeSet.add(new Node(
                    Murmur3_32Hash.getInstance().makeHash((host + port).getBytes(StandardCharsets.UTF_8)),
                    host,
                    Integer.parseInt(port)));
            nodesMap.put(listenerName, nodeSet);
        }

        return nodesMap;
    }

    @Getter
    static class KopEventWrapper {
        private final KopEvent kopEvent;
        private final long createdTime;

        public KopEventWrapper(KopEvent kopEvent) {
            this.kopEvent = kopEvent;
            this.createdTime = MathUtils.nowInNano();
        }

        @Override
        public String toString() {
            return "KopEventWrapper("
                    + "kopEvent=" + ((kopEvent == null) ? "null" : "'" + kopEvent + "'")
                    + ", createdTime=" + "'" + createdTime + "'"
                    + ")";
        }

    }

    interface KopEvent {
        void process(BiConsumer<String, Long> registerEventLatency,
                     long startProcessTime);

        String name();
    }

    class DeleteTopicsEvent implements KopEvent {
        private final boolean isFromClient;
        private final List<String> topicsDeletions;
        private final EndPoint advertisedEndPoint;

        public DeleteTopicsEvent(boolean isFromClient,
                                 List<String> topicsDeletions,
                                 EndPoint advertisedEndPoint) {
            this.isFromClient = isFromClient;
            this.topicsDeletions = topicsDeletions;
            this.advertisedEndPoint = advertisedEndPoint;
        }

        @Override
        public void process(BiConsumer<String, Long> registerEventLatency,
                            long startProcessTime) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Delete topics listener fired for topics {} to be deleted", topicsDeletions);
                }

                // send topicsSuccessfulDeletions to other kop brokers which DeleteTopicsRequest from client
                if (isFromClient) {
                    log.info("DeleteTopicsRequest from user client, "
                            + "we need send {} to other kop brokers", topicsDeletions);
                    sendDeleteTopics(topicsDeletions);
                } else {
                    log.info("DeleteTopicsRequest from other broker. Only clean up the information of "
                            + "the local GroupMetadata for topics {}", topicsDeletions);
                }

                // Localize groupCoordinatorsByTenant to avoid multi-thread conflicts
                final Map<String, GroupCoordinator> currentCoordinators = new HashMap<>(groupCoordinatorsByTenant);

                currentCoordinators.forEach((tenant, groupCoordinator) -> {
                    if (groupCoordinator.isActive()) {
                        HashSet<String> topicsFullNameDeletionsSets = Sets.newHashSet();
                        HashSet<KopTopic> kopTopicsSet = Sets.newHashSet();
                        String namespacePrefix = MetadataUtils.constructMetadataNamespace(tenant, kafkaConfig);
                        topicsDeletions.forEach(topic -> {
                            KopTopic kopTopic = new KopTopic(TopicNameUtils.getTopicNameWithUrlDecoded(topic),
                                    namespacePrefix);
                            kopTopicsSet.add(kopTopic);
                            topicsFullNameDeletionsSets.add(kopTopic.getFullName());
                        });

                        Iterable<GroupMetadata> groupMetadataIterable = groupCoordinator
                                .getGroupManager().currentGroups();
                        HashSet<TopicPartition> topicPartitionsToBeDeletions = Sets.newHashSet();

                        groupMetadataIterable.forEach(groupMetadata -> {
                            topicPartitionsToBeDeletions.addAll(
                                    groupMetadata.collectPartitionsWithTopics(topicsFullNameDeletionsSets));
                        });

                        Set<String> curDeletedTopics = Sets.newHashSet();
                        if (!topicPartitionsToBeDeletions.isEmpty()) {
                            groupCoordinator.handleDeletedPartitions(topicPartitionsToBeDeletions);
                            Set<String> collectDeleteTopics = topicPartitionsToBeDeletions
                                    .stream()
                                    .map(TopicPartition::topic)
                                    .collect(Collectors.toSet());

                            curDeletedTopics = kopTopicsSet.stream().filter(
                                    kopTopic -> collectDeleteTopics.contains(kopTopic.getFullName())
                            ).map(KopTopic::getOriginalName).collect(Collectors.toSet());
                        }

                        log.info("Tenant {} GroupMetadata delete topics {}, no matching topics {}",
                                tenant, curDeletedTopics,
                                Sets.difference(topicsFullNameDeletionsSets, curDeletedTopics));
                    }
                });
            } finally {
                registerEventLatency.accept(name(), startProcessTime);
            }
        }

        private void sendDeleteTopics(List<String> topicsDeletions) {
            Collection<? extends Node> interBrokers = adminManager.getBrokers(kafkaConfig.getInterBrokerListenerName());
            List<AdminClient> adminClients = Lists.newArrayList();
            try {
                interBrokers.forEach(broker -> {
                    if (!advertisedEndPoint.getHostname().equals(broker.host())
                            || !(advertisedEndPoint.getPort() == broker.port())) {
                        AdminClient adminClient = getKafkaAdminClient(broker.host(), broker.port());
                        adminClients.add(adminClient);
                        adminClient.deleteTopics(topicsDeletions);
                        log.info("Send deleteTopics {} to kop broker {}:{}",
                                topicsDeletions, broker.host(), broker.port());
                    }
                });
            } finally {
                adminClients.forEach(AdminClient::close);
            }
        }

        private AdminClient getKafkaAdminClient(final String host, final int port) {
            final Properties properties = new Properties();
            properties.put(AdminClientConfig.CLIENT_ID_CONFIG, AdminManager.INTER_ADMIN_CLIENT_ID);
            properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
            properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, kafkaConfig.getInterBrokerSecurityProtocol());
            properties.put(SaslConfigs.SASL_MECHANISM, kafkaConfig.getSaslMechanismInterBrokerProtocol());

            return KafkaAdminClient.create(properties);
        }

        @Override
        public String name() {
            return "DeleteTopicsEvent";
        }
    }

    class BrokersChangeEvent implements KopEvent {
        @Override
        public void process(BiConsumer<String, Long> registerEventLatency,
                            long startProcessTime) {
            metadataStore.getChildren(getBrokersChangePath()).whenComplete(
                    (brokers, e) -> {
                        if (e != null) {
                            log.error("BrokersChangeEvent process have an error", e);
                            return;
                        }
                        getBrokers(brokers, registerEventLatency, name(), startProcessTime);
                    });
        }

        @Override
        public String name() {
            return "BrokersChangeEvent";
        }
    }

    static class ShutdownEventThread implements KopEvent {

        @Override
        public void process(BiConsumer<String, Long> registerEventLatency,
                            long startProcessTime) {
            // Here is only record shutdown KopEventThread event.
            registerEventLatency.accept(name(), startProcessTime);
        }

        @Override
        public String name() {
            return "ShutdownEventThread";
        }
    }

    public KopEventWrapper getDeleteTopicEvent(boolean isFromClient,
                                               List<String> topicsDeletions,
                                               EndPoint advertisedEndPoint) {
        return new KopEventWrapper(new DeleteTopicsEvent(isFromClient, topicsDeletions, advertisedEndPoint));
    }

    public KopEventWrapper getBrokersChangeEvent() {
        return new KopEventWrapper(new BrokersChangeEvent());
    }

    public KopEventWrapper getShutdownEventThread() {
        return new KopEventWrapper(new ShutdownEventThread());
    }

    public static String getBrokersChangePath() {
        return LoadManager.LOADBALANCE_BROKERS_ROOT;
    }

}
