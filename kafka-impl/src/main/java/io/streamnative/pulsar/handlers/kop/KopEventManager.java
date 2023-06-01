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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
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
    private final DeletionTopicsHandler deletionTopicsHandler;
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
        this.deletionTopicsHandler = new DeletionTopicsHandler(this);
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
                    log.debug("Shutting down KopEventThread.");
                } else {
                    eventWrapper.kopEvent.process(registerEventLatency, MathUtils.nowInNano());
                }
            } catch (InterruptedException e) {
                log.debug("Error processing event {}", eventWrapper, e);
            }
        }

    }

    private void registerChildChangeHandler() {
        metadataStore.registerListener(this::handleChildChangePathNotification);

        // Really register ChildChange notification.
        metadataStore.getChildren(getDeleteTopicsPath());
        // init local kop brokers cache
        getBrokers(metadataStore.getChildren(getBrokersChangePath()).join(),
                null, "", -1);
    }

    private void handleChildChangePathNotification(Notification notification) {
        if (notification.getPath().equals(LoadManager.LOADBALANCE_BROKERS_ROOT)) {
            this.brokersChangeHandler.handleChildChange();
        } else if (notification.getPath().equals(getDeleteTopicsPath())) {
            this.deletionTopicsHandler.handleChildChange();
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
            final AdvertisedListener advertisedListener = AdvertisedListener.create(kopBrokerStr);
            String listenerName = advertisedListener.getListenerName();
            String host = advertisedListener.getHostname();
            int port = advertisedListener.getPort();
            Set<Node> nodeSet = nodesMap.computeIfAbsent(listenerName, s -> new HashSet<>());
            nodeSet.add(new Node(
                    Murmur3_32Hash.getInstance().makeHash((host + port).getBytes(StandardCharsets.UTF_8)),
                    host,
                    port));
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

        @Override
        public void process(BiConsumer<String, Long> registerEventLatency,
                            long startProcessTime) {
            try {
                List<String> topicsDeletions = metadataStore.getChildren(getDeleteTopicsPath()).get();

                if (log.isDebugEnabled()) {
                    log.debug("Delete topics listener fired for topics {} to be deleted", topicsDeletions);
                }

                // Localize groupCoordinatorsByTenant to avoid multi-thread conflicts
                final Map<String, GroupCoordinator> currentCoordinators = new HashMap<>(groupCoordinatorsByTenant);
                final Set<String> deletedTopics = Sets.newConcurrentHashSet();
                final AtomicInteger pendingCoordinators = new AtomicInteger(currentCoordinators.size());

                currentCoordinators.forEach((tenant, groupCoordinator) -> {
                    if (groupCoordinator.isActive()) {
                        HashSet<String> topicsFullNameDeletionsSets = Sets.newHashSet();
                        HashSet<KopTopic> kopTopicsSet = Sets.newHashSet();
                        String namespacePrefix = MetadataUtils.constructUserTopicsNamespace(tenant, kafkaConfig);
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

                        deletedTopics.addAll(curDeletedTopics);
                    }
                    if (pendingCoordinators.decrementAndGet() == 0) {
                        deletedTopics.forEach(deletedTopic -> {
                            metadataStore.delete(
                                    getDeleteTopicsPath() + "/" + deletedTopic, Optional.of((long) -1));
                        });
                    }
                });
            } catch (ExecutionException | InterruptedException e) {
                log.error("DeleteTopicsEvent process have an error", e);
            } finally {
                registerEventLatency.accept(name(), startProcessTime);
            }
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

    public KopEventWrapper getDeleteTopicEvent() {
        return new KopEventWrapper(new DeleteTopicsEvent());
    }

    public KopEventWrapper getBrokersChangeEvent() {
        return new KopEventWrapper(new BrokersChangeEvent());
    }

    public KopEventWrapper getShutdownEventThread() {
        return new KopEventWrapper(new ShutdownEventThread());
    }

    public static String getKopPath() {
        return "/kop";
    }

    public static String getDeleteTopicsPath() {
        return getKopPath() + "/delete_topics";
    }

    public static String getBrokersChangePath() {
        return LoadManager.LOADBALANCE_BROKERS_ROOT;
    }

}
