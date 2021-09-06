package io.streamnative.pulsar.handlers.kop;

import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.KopZkClient;
import io.streamnative.pulsar.handlers.kop.utils.ShutdownableThread;
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.zookeeper.KeeperException;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class KopEventManager {
    private static final String END_POINT_SEPARATOR = ",";
    private static final String REGEX = "^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    private static final String kopEventThreadName = "kop-event-thread";
    private final ReentrantLock putLock = new ReentrantLock();
    private static final LinkedBlockingQueue<KopEvent> queue =
            new LinkedBlockingQueue<>();
    private final KopEventThread thread =
            new KopEventThread(kopEventThreadName);
    private final GroupCoordinator coordinator;
    private final KopZkClient kopZkClient;
    private final AdminManager adminManager;
    private final DeletionTopicsHandler deletionTopicsHandler;
    private final BrokersChangeHandler brokersChangeHandler;

    public KopEventManager(GroupCoordinator coordinator,
                           KopZkClient kopZkClient,
                           AdminManager adminManager) {
        this.coordinator = coordinator;
        this.kopZkClient = kopZkClient;
        this.adminManager = adminManager;
        this.deletionTopicsHandler = new DeletionTopicsHandler(this);
        this.brokersChangeHandler = new BrokersChangeHandler(this);
    }

    public void start() {
        registerZNodeChildChangeHandler();
        thread.start();
    }

    public void close() {
        try {
            thread.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted at shutting down {}", kopEventThreadName);
        }

    }


    public void put(KopEvent event) {
        try {
            putLock.lock();
            queue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Error put event {} to coordinator event queue {}", event, e);
        } finally {
            putLock.unlock();
        }
    }

    public void clearAndPut(KopEvent event) {
        try {
            putLock.lock();
            queue.clear();
            put(event);
        } finally {
            putLock.unlock();
        }
    }

    static class KopEventThread extends ShutdownableThread {

        public KopEventThread(String name) {
            super(name);
        }

        @Override
        protected void doWork() {
            KopEvent event = null;
            try {
                event = queue.take();
                event.process();
            } catch (InterruptedException e) {
                log.error("Error processing event {}, {}", event, e);
            }
        }

    }

    private void registerZNodeChildChangeHandler() {
        kopZkClient.registerZNodeChildChangeHandler(deletionTopicsHandler);
        kopZkClient.registerZNodeChildChangeHandler(brokersChangeHandler);
        try {
            // Really register ZNodeChildChange to zk.
            kopZkClient.getTopicDeletions();
            // init local kop brokers cache
            getBrokers(kopZkClient.getBrokers());
        } catch (InterruptedException | KeeperException e) {
            log.error("registerZNodeChildChangeHandler have failed with an error {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void getBrokers(List<String> pulsarBrokers) {
        HashSet<Node> kopBrokers = Sets.newHashSet();
        pulsarBrokers.forEach(broker -> {
            try {
                byte[] brokerInfo =
                        kopZkClient.getDataForPath(KopZkClient.getBrokersChangeZNodePath() + "/" + broker);
                JsonObject jsonObject = parseJsonObject(new String(brokerInfo));
                JsonObject protocols = jsonObject.getAsJsonObject("protocols");
                JsonElement element = protocols.get("kafka");

                if (element != null) {
                    String kopBrokerStr = element.getAsString();
                    Node kopNode = getNode(kopBrokerStr);
                    kopBrokers.add(kopNode);
                }
            } catch (Exception e) {
                log.error("Get broker {} ZNode data failed which have an error {}", broker, e.getMessage());
                e.printStackTrace();
            }
        });
        Collection<? extends Node> oldKopBrokers = adminManager.getBrokers();
        adminManager.addBrokers(kopBrokers);
        log.info("Refresh kop brokers new cache {}, old brokers cache {}",
                adminManager.getBrokers(), oldKopBrokers);
    }

    private JsonObject parseJsonObject(String info) {
        JsonParser parser = new JsonParser();
        return parser.parse(info).getAsJsonObject();
    }

    private Node getNode(String kopBrokerStr) {
        final String errorMessage = "kopBrokerStr " + kopBrokerStr + " is invalid";
        final Matcher matcher = PATTERN.matcher(kopBrokerStr);
        checkState(matcher.find(), errorMessage);
        checkState(matcher.groupCount() == 3, errorMessage);
        String host = matcher.group(2);
        String port = matcher.group(3);

        return new Node(
                Murmur3_32Hash.getInstance().makeHash((host + port).getBytes(UTF_8)),
                host,
                Integer.parseInt(port));
    }

    class DeletionTopicsHandler implements ZooKeeperClient.ZNodeChildChangeHandler {
        private final KopEventManager kopEventManager;

        public DeletionTopicsHandler(KopEventManager kopEventManager) {
            this.kopEventManager = kopEventManager;
        }

        @Override
        public String path() {
            return KopZkClient.getDeleteTopicsZNodePath();
        }

        @Override
        public void handleChildChange() {
            kopEventManager.put(new DeleteTopicsEvent());
        }
    }

    class BrokersChangeHandler implements ZooKeeperClient.ZNodeChildChangeHandler {
        private final KopEventManager kopEventManager;

        public BrokersChangeHandler(KopEventManager kopEventManager) {
            this.kopEventManager = kopEventManager;
        }

        @Override
        public String path() {
            return KopZkClient.getBrokersChangeZNodePath();
        }

        @Override
        public void handleChildChange() {
            kopEventManager.put(new BrokersChangeEvent());
        }

    }


    interface KopEvent {
        void process();
    }

    class DeleteTopicsEvent implements KopEvent {

        @Override
        public void process() {
            if (!coordinator.isActive()) {
                return;
            }

            try {
                List<String> topicsDeletions = kopZkClient.getTopicDeletions();

                HashSet<String> topicsFullNameDeletionsSets = Sets.newHashSet();
                HashSet<KopTopic> kopTopicsSet = Sets.newHashSet();
                topicsDeletions.forEach(topic -> {
                    KopTopic kopTopic = new KopTopic(topic);
                    kopTopicsSet.add(kopTopic);
                    topicsFullNameDeletionsSets.add(kopTopic.getFullName());
                });

                log.debug("Delete topics listener fired for topics {} to be deleted", topicsDeletions);
                Iterable<GroupMetadata> groupMetadataIterable = coordinator.getGroupManager().currentGroups();
                HashSet<TopicPartition> topicPartitionsToBeDeletions = Sets.newHashSet();

                groupMetadataIterable.forEach(groupMetadata -> {
                    topicPartitionsToBeDeletions.addAll(
                            groupMetadata.collectPartitionsWithTopics(topicsFullNameDeletionsSets));
                });

                Set<String> deletedTopics = Sets.newHashSet();
                if (!topicPartitionsToBeDeletions.isEmpty()) {
                    coordinator.handleDeletedPartitions(topicPartitionsToBeDeletions);
                    Set<String> collectDeleteTopics = topicPartitionsToBeDeletions
                            .stream()
                            .map(TopicPartition::topic)
                            .collect(Collectors.toSet());

                    deletedTopics = kopTopicsSet.stream().filter(
                            kopTopic -> collectDeleteTopics.contains(kopTopic.getFullName())
                    ).map(KopTopic::getOriginalName).collect(Collectors.toSet());

                    kopZkClient.deleteNodesForPaths(
                            KopZkClient.getDeleteTopicsZNodePath(), deletedTopics);
                }

                log.info("GroupMetadata delete topics {}, no matching topics {}",
                        deletedTopics, Sets.difference(topicsFullNameDeletionsSets, deletedTopics));

            } catch (Exception e) {
                log.error("DeleteTopicsEvent process have an error {}", e.getMessage());
                e.printStackTrace();
            }
        }
    }

    class BrokersChangeEvent implements KopEvent {
        @Override
        public void process() {
            try {
                getBrokers(kopZkClient.getBrokers());
            } catch (Exception e) {
                log.error("BrokersChangeEvent process have an error {}", e.getMessage());
                e.printStackTrace();
            }
        }
    }

}
