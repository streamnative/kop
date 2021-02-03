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
import static io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils.getKafkaTopicNameFromPulsarTopicname;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.OffsetConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Kafka Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class KafkaProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "kafka";
    public static final String TLS_HANDLER = "tls";

    /**
     * Listener for the changing of topic that stores offsets of consumer group.
     */
    public static class OffsetAndTopicListener implements NamespaceBundleOwnershipListener {

        final BrokerService service;
        final NamespaceName kafkaMetaNs;
        final NamespaceName kafkaTopicNs;
        final GroupCoordinator groupCoordinator;
        public OffsetAndTopicListener(BrokerService service,
                                   KafkaServiceConfiguration kafkaConfig,
                                   GroupCoordinator groupCoordinator) {
            this.service = service;
            this.kafkaMetaNs = NamespaceName
                .get(kafkaConfig.getKafkaMetadataTenant(), kafkaConfig.getKafkaMetadataNamespace());
            this.groupCoordinator = groupCoordinator;
            this.kafkaTopicNs = NamespaceName
                    .get(kafkaConfig.getKafkaTenant(), kafkaConfig.getKafkaNamespace());
        }

        @Override
        public void onLoad(NamespaceBundle bundle) {
            // 1. get new partitions owned by this pulsar service.
            // 2. load partitions by GroupCoordinator.handleGroupImmigration.
            service.pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("get owned topic list when onLoad bundle {}, topic size {} ", bundle, topics.size());
                        for (String topic : topics) {
                            TopicName name = TopicName.get(topic);
                            // already filtered namespace, check the local name without partition
                            if (Topic.GROUP_METADATA_TOPIC_NAME.equals(getKafkaTopicNameFromPulsarTopicname(name))) {
                                checkState(name.isPartitioned(),
                                    "OffsetTopic should be partitioned in onLoad, but get " + name);

                                if (log.isDebugEnabled()) {
                                    log.debug("New offset partition load:  {}, broker: {}",
                                        name, service.pulsar().getBrokerServiceUrl());
                                }
                                groupCoordinator.handleGroupImmigration(name.getPartitionIndex());
                            }
                            KafkaTopicManager.removeTopicManagerCache(name.toString());
                            // update lookup cache when onload
                            try {
                                CompletableFuture<InetSocketAddress> retFuture = new CompletableFuture<>();
                                ((PulsarClientImpl) service.pulsar().getClient()).getLookup()
                                        .getBroker(TopicName.get(topic))
                                        .whenComplete((pair, throwable) -> {
                                            if (throwable != null) {
                                                log.warn("cloud not get broker", throwable);
                                                retFuture.complete(null);
                                            }
                                            checkState(pair.getLeft().equals(pair.getRight()));
                                            retFuture.complete(pair.getLeft());
                                        });
                                KafkaTopicManager.LOOKUP_CACHE.put(topic, retFuture);
                            } catch (PulsarServerException e) {
                                log.error("onLoad PulsarServerException ", e);
                            }
                        }
                    } else {
                        log.error("Failed to get owned topic list for "
                            + "OffsetAndTopicListener when triggering on-loading bundle {}.",
                            bundle, ex);
                    }
                });
        }

        @Override
        public void unLoad(NamespaceBundle bundle) {
            // 1. get partitions owned by this pulsar service.
            // 2. remove partitions by groupCoordinator.handleGroupEmigration.
            service.pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("get owned topic list when unLoad bundle {}, topic size {} ", bundle, topics.size());
                        for (String topic : topics) {
                            TopicName name = TopicName.get(topic);

                            // already filtered namespace, check the local name without partition
                            if (Topic.GROUP_METADATA_TOPIC_NAME.equals(getKafkaTopicNameFromPulsarTopicname(name))) {
                                checkState(name.isPartitioned(),
                                    "OffsetTopic should be partitioned in unLoad, but get " + name);

                                if (log.isDebugEnabled()) {
                                    log.debug("Offset partition unload:  {}, broker: {}",
                                        name, service.pulsar().getBrokerServiceUrl());
                                }
                                groupCoordinator.handleGroupEmigration(name.getPartitionIndex());
                            }
                            // remove cache when unload
                            KafkaTopicManager.removeTopicManagerCache(name.toString());
                        }
                    } else {
                        log.error("Failed to get owned topic list for "
                            + "OffsetAndTopicListener when triggering un-loading bundle {}.",
                            bundle, ex);
                    }
                });
        }

        // verify that this bundle is served by this broker,
        // and namespace is related to kafka metadata namespace
        @Override
        public boolean test(NamespaceBundle namespaceBundle) {
            return namespaceBundle.getNamespaceObject().equals(kafkaMetaNs)
                    || namespaceBundle.getNamespaceObject().equals(kafkaTopicNs);
        }

    }
    /**
     * Kafka Listener Type.
     */
    public enum ListenerType {
        PLAINTEXT,
        SSL
    }

    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    @Getter
    private BrokerService brokerService;
    @Getter
    private GroupCoordinator groupCoordinator;
    @Getter
    private TransactionCoordinator transactionCoordinator;
    @Getter
    private String bindAddress;


    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equals(protocol.toLowerCase());
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // init config
        if (conf instanceof KafkaServiceConfiguration) {
            // in unit test, passed in conf will be KafkaServiceConfiguration
            kafkaConfig = (KafkaServiceConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            kafkaConfig = ConfigurationUtils.create(conf.getProperties(), KafkaServiceConfiguration.class);

            // some of the configs value in conf.properties may not updated.
            // So need to get latest value from conf itself
            kafkaConfig.setAdvertisedAddress(conf.getAdvertisedAddress());
            kafkaConfig.setBindAddress(conf.getBindAddress());
        }
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(kafkaConfig.getBindAddress());
        KopTopic.initialize(kafkaConfig.getKafkaTenant() + "/" + kafkaConfig.getKafkaNamespace());
    }

    // This method is called after initialize
    @Override
    public String getProtocolDataToAdvertise() {
        return kafkaConfig.getKafkaAdvertisedListeners();
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;

        log.info("Starting KafkaProtocolHandler, kop version is: '{}'", KopVersion.getVersion());
        log.info("Git Revision {}", KopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
            KopVersion.getBuildUser(),
            KopVersion.getBuildHost(),
            KopVersion.getBuildTime());

        // init and start group coordinator
        if (kafkaConfig.isEnableGroupCoordinator()) {
            try {
                initGroupCoordinator(brokerService);
                startGroupCoordinator();
                // and listener for Offset topics load/unload
                brokerService.pulsar()
                    .getNamespaceService()
                    .addNamespaceBundleOwnershipListener(
                        new OffsetAndTopicListener(brokerService, kafkaConfig, groupCoordinator));
            } catch (Exception e) {
                log.error("initGroupCoordinator failed with", e);
            }
        }
        if (kafkaConfig.isEnableTransactionCoordinator()) {
            try {
                initTransactionCoordinator();
            } catch (Exception e) {
                log.error("Initialized transaction coordinator failed.", e);
            }
        }
    }

    // this is called after initialize, and with kafkaConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(kafkaConfig != null);
        checkState(brokerService != null);
        if (kafkaConfig.isEnableGroupCoordinator()) {
            checkState(groupCoordinator != null);
        }

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            final Map<SecurityProtocol, EndPoint> advertisedEndpointMap =
                    EndPoint.parseListeners(kafkaConfig.getKafkaAdvertisedListeners());
            EndPoint.parseListeners(kafkaConfig.getListeners()).forEach((protocol, endPoint) -> {
                EndPoint advertisedEndPoint = advertisedEndpointMap.get(protocol);
                if (advertisedEndPoint == null) {
                    // Use the bind endpoint as the advertised endpoint.
                    advertisedEndPoint = endPoint;
                }
                switch (protocol) {
                    case PLAINTEXT:
                    case SASL_PLAINTEXT:
                        builder.put(endPoint.getInetAddress(), new KafkaChannelInitializer(brokerService.getPulsar(),
                                kafkaConfig, groupCoordinator, transactionCoordinator, false, advertisedEndPoint));
                        break;
                    case SSL:
                    case SASL_SSL:
                        builder.put(endPoint.getInetAddress(), new KafkaChannelInitializer(brokerService.getPulsar(),
                                kafkaConfig, groupCoordinator, transactionCoordinator, true, advertisedEndPoint));
                        break;
                }
            });

            return builder.build();
        } catch (Exception e){
            log.error("KafkaProtocolHandler newChannelInitializers failed with ", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (groupCoordinator != null) {
            groupCoordinator.shutdown();
        }
        KafkaTopicManager.LOOKUP_CACHE.clear();
    }

    public void initGroupCoordinator(BrokerService service) throws Exception {
        GroupConfig groupConfig = new GroupConfig(
            kafkaConfig.getGroupMinSessionTimeoutMs(),
            kafkaConfig.getGroupMaxSessionTimeoutMs(),
            kafkaConfig.getGroupInitialRebalanceDelayMs()
        );

        OffsetConfig offsetConfig = OffsetConfig.builder()
            .offsetsTopicName(kafkaConfig.getKafkaMetadataTenant() + "/"
                + kafkaConfig.getKafkaMetadataNamespace()
                + "/" + Topic.GROUP_METADATA_TOPIC_NAME)
            .offsetsTopicNumPartitions(kafkaConfig.getOffsetsTopicNumPartitions())
            .offsetsTopicCompressionType(CompressionType.valueOf(kafkaConfig.getOffsetsTopicCompressionCodec()))
            .maxMetadataSize(kafkaConfig.getOffsetMetadataMaxSize())
            .offsetsRetentionCheckIntervalMs(kafkaConfig.getOffsetsRetentionCheckIntervalMs())
            .offsetsRetentionMs(TimeUnit.MINUTES.toMillis(kafkaConfig.getOffsetsRetentionMinutes()))
            .build();

        PulsarAdmin pulsarAdmin = service.pulsar().getAdminClient();
        MetadataUtils.createKafkaMetadataIfMissing(pulsarAdmin, kafkaConfig);


        this.groupCoordinator = GroupCoordinator.of(
            brokerService,
            (PulsarClientImpl) (service.pulsar().getClient()),
            groupConfig,
            offsetConfig,
            kafkaConfig,
            SystemTimer.builder()
                .executorName("group-coordinator-timer")
                .build(),
            Time.SYSTEM
        );

        loadOffsetTopics(groupCoordinator);
    }

    public void startGroupCoordinator() throws Exception {
        if (this.groupCoordinator != null) {
            this.groupCoordinator.startup(true);
        } else {
            log.error("Failed to start group coordinator. Need init it first.");
        }
    }

    public void initTransactionCoordinator() {
        TransactionConfig transactionConfig = TransactionConfig.builder().build();
        this.transactionCoordinator = TransactionCoordinator.of(transactionConfig);
    }

    /**
     * This method discovers ownership of offset topic partitions and attempts to load offset topics
     * assigned to this broker.
     */
    private void loadOffsetTopics(GroupCoordinator groupCoordinator) throws Exception {
        Lookup lookupService = brokerService.pulsar().getAdminClient().lookups();
        String currentBroker = brokerService.pulsar().getBrokerServiceUrl();
        String topicBase = MetadataUtils.constructOffsetsTopicBaseName(kafkaConfig);
        int numPartitions = kafkaConfig.getOffsetsTopicNumPartitions();

        Map<String, List<Integer>> mapBrokerToPartition = new HashMap<>();

        for (int i = 0; i < numPartitions; i++) {
            String broker = lookupService.lookupTopic(topicBase + PARTITIONED_TOPIC_SUFFIX + i);
            mapBrokerToPartition.putIfAbsent(broker, new ArrayList());
            mapBrokerToPartition.get(broker).add(i);
        }

        mapBrokerToPartition.entrySet().stream().forEach(
            e -> log.info("Discovered broker: {} owns offset topic partitions: {} ", e.getKey(), e.getValue()));

        List<Integer> partitionsOwnedByCurrentBroker = mapBrokerToPartition.get(currentBroker);

        if (null != partitionsOwnedByCurrentBroker && !partitionsOwnedByCurrentBroker.isEmpty()) {
            List<CompletableFuture<Void>> lists = partitionsOwnedByCurrentBroker.stream().map(
                (ii) -> groupCoordinator.handleGroupImmigration(ii)).collect(Collectors.toList());

            FutureUtil.waitForAll(lists).get();
        } else {
            log.info("Current broker: {} does not own any of the offset topic partitions", currentBroker);
        }
    }
}
