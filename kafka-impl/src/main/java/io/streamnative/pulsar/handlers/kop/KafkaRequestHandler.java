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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.ListenerType.PLAINTEXT;
import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.ListenerType.SSL;
import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.getKopBrokerUrl;
import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.getListenerPort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupOverview;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupSummary;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import io.streamnative.pulsar.handlers.kop.utils.OffsetFinder;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.naming.AuthenticationException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMember;
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListGroupsResponse.Group;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;

/**
 * This class contains all the request handling methods.
 */
@Slf4j
@Getter
public class KafkaRequestHandler extends KafkaCommandDecoder {
    public static final long DEFAULT_TIMESTAMP = 0L;

    private final PulsarService pulsarService;
    private final KafkaServiceConfiguration kafkaConfig;
    private final KafkaTopicManager topicManager;
    private final GroupCoordinator groupCoordinator;

    private final String clusterName;
    private final ScheduledExecutorService executor;
    private final PulsarAdmin admin;
    private final SaslAuthenticator authenticator;
    private final AdminManager adminManager;

    private final Boolean tlsEnabled;
    private final String localListeners;
    private final int plaintextPort;
    private final int sslPort;
    private final int defaultNumPartitions;
    public final int maxReadEntriesNum;

    private final Map<TopicPartition, PendingProduceQueue> pendingProduceQueueMap = new ConcurrentHashMap<>();

    public KafkaRequestHandler(PulsarService pulsarService,
                               KafkaServiceConfiguration kafkaConfig,
                               GroupCoordinator groupCoordinator,
                               Boolean tlsEnabled) throws Exception {
        super();
        this.pulsarService = pulsarService;
        this.kafkaConfig = kafkaConfig;
        this.groupCoordinator = groupCoordinator;
        this.clusterName = kafkaConfig.getClusterName();
        this.executor = pulsarService.getExecutor();
        this.admin = pulsarService.getAdminClient();
        final boolean authenticationEnabled = pulsarService.getBrokerService().isAuthenticationEnabled()
                && !kafkaConfig.getSaslAllowedMechanisms().isEmpty();
        this.authenticator = authenticationEnabled
                ? new SaslAuthenticator(pulsarService, kafkaConfig.getSaslAllowedMechanisms())
                : null;
        this.adminManager = new AdminManager(admin);
        this.tlsEnabled = tlsEnabled;
        this.localListeners = KafkaProtocolHandler.getListenersFromConfig(kafkaConfig);
        this.plaintextPort = getListenerPort(localListeners, PLAINTEXT);
        this.sslPort = getListenerPort(localListeners, SSL);
        this.topicManager = new KafkaTopicManager(this);
        this.defaultNumPartitions = kafkaConfig.getDefaultNumPartitions();
        this.maxReadEntriesNum = kafkaConfig.getMaxReadEntriesNum();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        getTopicManager().updateCtx();
        if (authenticator != null) {
            authenticator.reset();
        }
        log.info("channel active: {}", ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("channel inactive {}", ctx.channel());

        close();
        isActive.set(false);
    }

    @Override
    protected void close() {
        if (isActive.getAndSet(false)) {
            log.info("close channel {}", ctx.channel());
            writeAndFlushWhenInactiveChannel(ctx.channel());
            ctx.close();
            topicManager.close();
        }
    }

    @Override
    protected boolean hasAuthenticated(KafkaHeaderAndRequest request) {
        return authenticator == null || authenticator.complete();
    }

    @Override
    protected void authenticate(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                CompletableFuture<AbstractResponse> responseFuture) throws AuthenticationException {
        if (authenticator != null) {
            authenticator.authenticate(
                    kafkaHeaderAndRequest.getHeader(), kafkaHeaderAndRequest.getRequest(), responseFuture);
        }
    }

    protected void handleApiVersionsRequest(KafkaHeaderAndRequest apiVersionRequest,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersionRequest.getHeader().apiVersion())) {
            // Notify Client that API_VERSION is UNSUPPORTED.
            AbstractResponse apiResponse = overloadDefaultApiVersionsResponse(true);
            resultFuture.complete(apiResponse);
        } else {
            AbstractResponse apiResponse = overloadDefaultApiVersionsResponse(false);
            resultFuture.complete(apiResponse);
        }
    }

    protected ApiVersionsResponse overloadDefaultApiVersionsResponse(boolean unsupportedApiVersion) {
        List<ApiVersionsResponse.ApiVersion> versionList = new ArrayList<>();
        if (unsupportedApiVersion){
            return new ApiVersionsResponse(0, Errors.UNSUPPORTED_VERSION, versionList);
        } else {
            for (ApiKeys apiKey : ApiKeys.values()) {
                if (apiKey.minRequiredInterBrokerMagic <= RecordBatch.CURRENT_MAGIC_VALUE) {
                    switch (apiKey) {
                        case FETCH:
                            // V4 added MessageSets responses. We need to make sure RecordBatch format is not used
                            versionList.add(new ApiVersionsResponse.ApiVersion((short) 1, (short) 4,
                                    apiKey.latestVersion()));
                            break;
                        case LIST_OFFSETS:
                            // V0 is needed for librdkafka
                            versionList.add(new ApiVersionsResponse.ApiVersion((short) 2, (short) 0,
                                    apiKey.latestVersion()));
                            break;
                        default:
                            versionList.add(new ApiVersionsResponse.ApiVersion(apiKey));
                    }
                }
            }
            return new ApiVersionsResponse(0, Errors.NONE, versionList);
        }
    }

    protected void handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                               CompletableFuture<AbstractResponse> resultFuture) {
        String err = String.format("Kafka API (%s) Not supported by kop server.",
            kafkaHeaderAndRequest.getHeader().apiKey());
        log.error(err);

        AbstractResponse apiResponse = kafkaHeaderAndRequest.getRequest()
            .getErrorResponse(new UnsupportedOperationException(err));
        resultFuture.complete(apiResponse);
    }

    protected void handleInactive(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                  CompletableFuture<AbstractResponse> resultFuture) {
        AbstractRequest request = kafkaHeaderAndRequest.getRequest();
        AbstractResponse apiResponse = request.getErrorResponse(new LeaderNotAvailableException("Channel is closing!"));

        log.error("Kafka API {} is send to a closing channel", kafkaHeaderAndRequest.getHeader().apiKey());

        resultFuture.complete(apiResponse);
    }

    // Leverage pulsar admin to get partitioned topic metadata
    private CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String topicName) {
        return admin.topics().getPartitionedTopicMetadataAsync(topicName);
    }

    // Get all topics exclude `__consumer_offsets`, the result of `topicMapFuture` is a map:
    //   key: the full topic name without partition suffix, e.g. persistent://public/default/my-topic
    //   value: the partitions associated with the key, e.g. for a topic with 3 partitions,
    //     persistent://public/default/my-topic-partition-0
    //     persistent://public/default/my-topic-partition-1
    //     persistent://public/default/my-topic-partition-2
    private void getAllTopicsAsync(CompletableFuture<Map<String, List<TopicName>>> topicMapFuture) {
        final String offsetsTopicName = new KopTopic(String.join("/",
                kafkaConfig.getKafkaMetadataTenant(),
                kafkaConfig.getKafkaMetadataNamespace(),
                Topic.GROUP_METADATA_TOPIC_NAME)
        ).getFullName();
        admin.tenants().getTenantsAsync().thenApply(tenants -> {
            if (tenants.isEmpty()) {
                topicMapFuture.complete(Maps.newHashMap());
                return null;
            }
            Map<String, List<TopicName>> topicMap = Maps.newConcurrentMap();

            AtomicInteger numTenants = new AtomicInteger(tenants.size());
            for (String tenant : tenants) {
                admin.namespaces().getNamespacesAsync(tenant).thenApply(namespaces -> {
                    if (namespaces.isEmpty() && numTenants.decrementAndGet() == 0) {
                        topicMapFuture.complete(topicMap);
                        return null;
                    }
                    AtomicInteger numNamespaces = new AtomicInteger(namespaces.size());
                    for (String namespace : namespaces) {
                        pulsarService.getNamespaceService().getListOfPersistentTopics(NamespaceName.get(namespace))
                                .thenApply(topics -> {
                                    for (String topic : topics) {
                                        TopicName topicName = TopicName.get(topic);
                                        String key = topicName.getPartitionedTopicName();
                                        // ignore the `__consumer_offsets` topic
                                        if (key.equals(offsetsTopicName)) {
                                            continue;
                                        }
                                        topicMap.computeIfAbsent(KopTopic.removeDefaultNamespacePrefix(key), ignored ->
                                                Collections.synchronizedList(new ArrayList<>())
                                        ).add(topicName);
                                    }
                                    if (numNamespaces.decrementAndGet() == 0 && numTenants.decrementAndGet() == 0) {
                                        topicMapFuture.complete(topicMap);
                                    }
                                    return null;
                                });
                    }
                    return null;
                });
            }
            return null;
        }).exceptionally(topicMapFuture::completeExceptionally);
    }

    protected void handleTopicMetadataRequest(KafkaHeaderAndRequest metadataHar,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(metadataHar.getRequest() instanceof MetadataRequest);

        MetadataRequest metadataRequest = (MetadataRequest) metadataHar.getRequest();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Request {}: for topic {} ",
                ctx.channel(), metadataHar.getHeader(), metadataRequest.topics());
        }

        // Command response for all topics
        List<TopicMetadata> allTopicMetadata = Collections.synchronizedList(Lists.newArrayList());
        List<Node> allNodes = Collections.synchronizedList(Lists.newArrayList());

        List<String> topics = metadataRequest.topics();
        // topics in format : persistent://%s/%s/abc-partition-x, will be grouped by as:
        //      Entry<abc, List[TopicName]>

        // A future for a map from <kafka topic> to <pulsarPartitionTopics>:
        //      e.g. <topic1, {persistent://public/default/topic1-partition-0,...}>
        //   1. no topics provided, get all topics from namespace;
        //   2. topics provided, get provided topics.
        CompletableFuture<Map<String, List<TopicName>>> pulsarTopicsFuture = new CompletableFuture<>();

        if (topics == null || topics.isEmpty()) {
            // clean all cache when get all metadata for librdkafka(<1.0.0).
            topicManager.clearTopicManagerCache();
            // get all topics
            getAllTopicsAsync(pulsarTopicsFuture);
        } else {
            // get only the provided topics
            Map<String, List<TopicName>> pulsarTopics = Maps.newHashMap();

            List<String> requestTopics = metadataRequest.topics();
            final int topicsNumber = requestTopics.size();
            AtomicInteger topicsCompleted = new AtomicInteger(0);

            requestTopics.stream()
                .forEach(topic -> {
                    KopTopic kopTopic = new KopTopic(topic);

                    // get partition numbers for each topic.
                    // If topic doesn't exist and allowAutoTopicCreation is enabled, the topic will be created first.
                    getPartitionedTopicMetadataAsync(kopTopic.getFullName())
                        .whenComplete((partitionedTopicMetadata, throwable) -> {
                            if (throwable != null) {
                                // Failed get partitions.
                                allTopicMetadata.add(
                                    new TopicMetadata(
                                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                        topic,
                                        false,
                                        Collections.emptyList()));
                                log.warn("[{}] Request {}: Failed to get partitioned pulsar topic {} metadata: {}",
                                        ctx.channel(), metadataHar.getHeader(),
                                        kopTopic.getFullName(), throwable.getMessage());
                            } else {
                                List<TopicName> pulsarTopicNames;
                                if (partitionedTopicMetadata.partitions > 0) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Topic {} has {} partitions",
                                            topic, partitionedTopicMetadata.partitions);
                                    }
                                    pulsarTopicNames = IntStream
                                        .range(0, partitionedTopicMetadata.partitions)
                                        .mapToObj(i -> TopicName.get(kopTopic.getPartitionName(i)))
                                        .collect(Collectors.toList());
                                    pulsarTopics.put(topic, pulsarTopicNames);
                                } else {
                                    if (kafkaConfig.isAllowAutoTopicCreation()
                                            && metadataRequest.allowAutoTopicCreation()) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Request {}: Topic {} has single partition, "
                                                    + "auto create partitioned topic",
                                                ctx.channel(), metadataHar.getHeader(), topic);
                                        }
                                        admin.topics().createPartitionedTopicAsync(kopTopic.getFullName(),
                                                defaultNumPartitions);
                                        pulsarTopicNames = IntStream
                                            .range(0, defaultNumPartitions)
                                            .mapToObj(i -> TopicName.get(kopTopic.getPartitionName(i)))
                                            .collect(Collectors.toList());
                                        pulsarTopics.put(topic, pulsarTopicNames);

                                    } else {
                                        // NOTE: Currently no matter topic is a non-partitioned topic or topic doesn't
                                        // exist, the queried partitions from broker are both 0.
                                        // See https://github.com/apache/pulsar/issues/8813 for details.
                                        log.error("[{}] Request {}: Topic {} doesn't exist and it's not allowed to"
                                                        + "auto create partitioned topic",
                                                ctx.channel(), metadataHar.getHeader(), topic);
                                        // not allow to auto create topic, return unknown topic
                                        allTopicMetadata.add(
                                            new TopicMetadata(
                                                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                topic,
                                                false,
                                                Collections.emptyList()));
                                    }
                                }
                            }

                            // whether handled all topics get partitions
                            int completedTopics = topicsCompleted.incrementAndGet();
                            if (completedTopics == topicsNumber) {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Request {}: Completed get {} topic's partitions",
                                        ctx.channel(), metadataHar.getHeader(), topicsNumber);
                                }
                                pulsarTopicsFuture.complete(pulsarTopics);
                            }
                        });
                });
        }

        // 2. After get all topics, for each topic, get the service Broker for it, and add to response
        AtomicInteger topicsCompleted = new AtomicInteger(0);
        // Each Pulsar broker can manage metadata like controller in Kafka, Kafka's AdminClient needs to find a
        // controller node for metadata management. So here we return the broker itself as a controller.
        final int controllerId = newSelfNode().id();
        pulsarTopicsFuture.whenComplete((pulsarTopics, e) -> {
            if (e != null) {
                log.warn("[{}] Request {}: Exception fetching metadata, will return null Response",
                    ctx.channel(), metadataHar.getHeader(), e);
                allNodes.add(newSelfNode());
                MetadataResponse finalResponse =
                    new MetadataResponse(
                        allNodes,
                        clusterName,
                        controllerId,
                        Collections.emptyList());
                resultFuture.complete(finalResponse);
                return;
            }

            final int topicsNumber = pulsarTopics.size();

            if (topicsNumber == 0) {
                // no topic partitions added, return now.
                allNodes.add(newSelfNode());
                MetadataResponse finalResponse =
                    new MetadataResponse(
                        allNodes,
                        clusterName,
                        controllerId,
                        allTopicMetadata);
                resultFuture.complete(finalResponse);
                return;
            }

            pulsarTopics.forEach((topic, list) -> {
                final int partitionsNumber = list.size();
                AtomicInteger partitionsCompleted = new AtomicInteger(0);
                List<PartitionMetadata> partitionMetadatas = Collections
                    .synchronizedList(Lists.newArrayListWithExpectedSize(partitionsNumber));

                list.forEach(topicName ->
                    findBroker(topicName)
                        .whenComplete(((partitionMetadata, throwable) -> {
                            if (throwable != null || partitionMetadata == null) {
                                log.warn("[{}] Request {}: Exception while find Broker metadata",
                                    ctx.channel(), metadataHar.getHeader(), throwable);
                                partitionMetadatas.add(newFailedPartitionMetadata(topicName));
                            } else {
                                Node newNode = partitionMetadata.leader();
                                synchronized (allNodes) {
                                    if (!allNodes.stream().anyMatch(node1 -> node1.equals(newNode))) {
                                        allNodes.add(newNode);
                                    }
                                }
                                partitionMetadatas.add(partitionMetadata);
                            }

                            // whether completed this topic's partitions list.
                            int finishedPartitions = partitionsCompleted.incrementAndGet();
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Request {}: FindBroker for topic {}, partitions found/all: {}/{}.",
                                    ctx.channel(), metadataHar.getHeader(),
                                    topic, finishedPartitions, partitionsNumber);
                            }
                            if (finishedPartitions == partitionsNumber) {
                                // new TopicMetadata for this topic
                                allTopicMetadata.add(
                                    new TopicMetadata(
                                        Errors.NONE,
                                        // The topic returned to Kafka clients should be the same with what it sent
                                        topic,
                                        false,
                                        partitionMetadatas));

                                // whether completed all the topics requests.
                                int finishedTopics = topicsCompleted.incrementAndGet();
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Request {}: Completed findBroker for topic {}, "
                                            + "partitions found/all: {}/{}. \n dump All Metadata:",
                                        ctx.channel(), metadataHar.getHeader(), topic,
                                        finishedTopics, topicsNumber);

                                    allTopicMetadata.stream()
                                        .forEach(data -> log.debug("TopicMetadata response: {}", data.toString()));
                                }
                                if (finishedTopics == topicsNumber) {
                                    // TODO: confirm right value for controller_id
                                    MetadataResponse finalResponse =
                                        new MetadataResponse(
                                            allNodes,
                                            clusterName,
                                            controllerId,
                                            allTopicMetadata);
                                    resultFuture.complete(finalResponse);
                                }
                            }
                        })));
            });
        });
    }

    protected void handleProduceRequest(KafkaHeaderAndRequest produceHar,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(produceHar.getRequest() instanceof ProduceRequest);
        ProduceRequest produceRequest = (ProduceRequest) produceHar.getRequest();
        if (produceRequest.transactionalId() != null) {
            log.warn("[{}] Transactions not supported", ctx.channel());

            resultFuture.complete(
                failedResponse(produceHar, new UnsupportedOperationException("No transaction support")));
            return;
        }

        Map<TopicPartition, CompletableFuture<PartitionResponse>> responsesFutures = new HashMap<>();

        final int responsesSize = produceRequest.partitionRecordsOrFail().size();

        final long dataSizePerPartition = produceHar.getBuffer().readableBytes();
        topicManager.getInternalServerCnx().increasePublishBuffer(dataSizePerPartition);

        // TODO: handle un-exist topic:
        //     nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
        for (Map.Entry<TopicPartition, ? extends Records> entry : produceRequest.partitionRecordsOrFail().entrySet()) {
            TopicPartition topicPartition = entry.getKey();

            CompletableFuture<PartitionResponse> partitionResponse = new CompletableFuture<>();
            responsesFutures.put(topicPartition, partitionResponse);

            if (log.isDebugEnabled()) {
                log.debug("[{}] Request {}: Produce messages for topic {} partition {}, request size: {} ",
                        ctx.channel(), produceHar.getHeader(),
                        topicPartition.topic(), topicPartition.partition(), responsesSize);
            }

            MemoryRecords records = (MemoryRecords) entry.getValue();
            String fullPartitionName = KopTopic.toString(topicPartition);
            PendingProduce pendingProduce =
                    new PendingProduce(partitionResponse, topicManager, fullPartitionName, records, executor);
            PendingProduceQueue queue =
                    pendingProduceQueueMap.computeIfAbsent(topicPartition, ignored -> new PendingProduceQueue());
            queue.add(pendingProduce);
            pendingProduce.whenComplete(queue::sendCompletedProduces);
        }

        CompletableFuture.allOf(responsesFutures.values().toArray(new CompletableFuture<?>[responsesSize]))
                .whenComplete((ignore, ex) -> {
                    topicManager.getInternalServerCnx().decreasePublishBuffer(dataSizePerPartition);
                    // all ex has translated to PartitionResponse with Errors.KAFKA_STORAGE_ERROR
                    Map<TopicPartition, PartitionResponse> responses = new ConcurrentHashMap<>();
                    for (Map.Entry<TopicPartition, CompletableFuture<PartitionResponse>> entry :
                            responsesFutures.entrySet()) {
                        responses.put(entry.getKey(), entry.getValue().join());
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Request {}: Complete handle produce.",
                                ctx.channel(), produceHar.toString());
                    }
                    resultFuture.complete(new ProduceResponse(responses));
                });
    }

    protected void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator,
                                                CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(findCoordinator.getRequest() instanceof FindCoordinatorRequest);
        FindCoordinatorRequest request = (FindCoordinatorRequest) findCoordinator.getRequest();

        if (request.coordinatorType() == FindCoordinatorRequest.CoordinatorType.GROUP) {
            int partition = groupCoordinator.partitionFor(request.coordinatorKey());
            String pulsarTopicName = groupCoordinator.getTopicPartitionName(partition);

            findBroker(TopicName.get(pulsarTopicName))
                .whenComplete((node, t) -> {
                    if (t != null || node == null){
                        log.error("[{}] Request {}: Error while find coordinator, .",
                            ctx.channel(), findCoordinator.getHeader(), t);

                        AbstractResponse response = new FindCoordinatorResponse(
                            Errors.LEADER_NOT_AVAILABLE,
                            Node.noNode());
                        resultFuture.complete(response);
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Found node {} as coordinator for key {} partition {}.",
                            ctx.channel(), node.leader(), request.coordinatorKey(), partition);
                    }

                    AbstractResponse response = new FindCoordinatorResponse(
                        Errors.NONE,
                        node.leader());
                    resultFuture.complete(response);
                });
        } else {
            throw new NotImplementedException("FindCoordinatorRequest not support TRANSACTION type "
                + request.coordinatorType());
        }
    }

    protected void handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(offsetFetch.getRequest() instanceof OffsetFetchRequest);
        OffsetFetchRequest request = (OffsetFetchRequest) offsetFetch.getRequest();
        checkState(groupCoordinator != null,
            "Group Coordinator not started");

        KeyValue<Errors, Map<TopicPartition, OffsetFetchResponse.PartitionData>> keyValue =
            groupCoordinator.handleFetchOffsets(
                request.groupId(),
                Optional.ofNullable(request.partitions())
            );

        resultFuture.complete(new OffsetFetchResponse(keyValue.getKey(), keyValue.getValue()));
    }

    private CompletableFuture<ListOffsetResponse.PartitionData>
    fetchOffsetForTimestamp(CompletableFuture<PersistentTopic> persistentTopic, Long timestamp, boolean legacyMode) {
        CompletableFuture<ListOffsetResponse.PartitionData> partitionData = new CompletableFuture<>();

        persistentTopic.whenComplete((perTopic, t) -> {
            if (t != null || perTopic == null) {
                log.error("Failed while get persistentTopic topic: {} ts: {}. ",
                    perTopic == null ? "null" : perTopic.getName(), timestamp, t);

                partitionData.complete(new ListOffsetResponse.PartitionData(
                    Errors.LEADER_NOT_AVAILABLE,
                    ListOffsetResponse.UNKNOWN_TIMESTAMP,
                    ListOffsetResponse.UNKNOWN_OFFSET));
                return;
            }

            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) perTopic.getManagedLedger();
            if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP) {
                PositionImpl position = (PositionImpl) managedLedger.getLastConfirmedEntry();
                if (log.isDebugEnabled()) {
                    log.debug("Get latest position for topic {} time {}. result: {}",
                        perTopic.getName(), timestamp, position);
                }

                // no entry in ledger, then entry id could be -1
                long entryId = position.getEntryId();

                if (legacyMode) {
                    partitionData.complete(new ListOffsetResponse.PartitionData(
                            Errors.NONE,
                            Collections.singletonList(MessageIdUtils
                                    .getOffset(position.getLedgerId(), entryId == -1 ? 0 : entryId))));

                } else {
                    partitionData.complete(new ListOffsetResponse.PartitionData(
                            Errors.NONE,
                            DEFAULT_TIMESTAMP,
                            MessageIdUtils
                                    .getOffset(position.getLedgerId(), entryId == -1 ? 0 : entryId)));
                }

            } else if (timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP) {
                PositionImpl position = OffsetFinder.getFirstValidPosition(managedLedger);

                if (log.isDebugEnabled()) {
                    log.debug("Get earliest position for topic {} time {}. result: {}",
                        perTopic.getName(), timestamp, position);
                }

                if (legacyMode) {
                    partitionData.complete(new ListOffsetResponse.PartitionData(
                            Errors.NONE,
                            Collections.singletonList(MessageIdUtils.getOffset(position.getLedgerId(),
                                    position.getEntryId()))));
                } else {
                    partitionData.complete(new ListOffsetResponse.PartitionData(
                            Errors.NONE,
                            DEFAULT_TIMESTAMP,
                            MessageIdUtils.getOffset(position.getLedgerId(), position.getEntryId())));
                }

            } else {
                // find with real wanted timestamp
                OffsetFinder offsetFinder = new OffsetFinder(managedLedger);

                offsetFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
                    @Override
                    public void findEntryComplete(Position position, Object ctx) {
                        PositionImpl finalPosition;
                        if (position == null) {
                            finalPosition = OffsetFinder.getFirstValidPosition(managedLedger);
                            if (finalPosition == null) {
                                log.warn("Unable to find position for topic {} time {}. get NULL position",
                                    perTopic.getName(), timestamp);

                                if (legacyMode) {
                                    partitionData.complete(new ListOffsetResponse
                                            .PartitionData(
                                            Errors.UNKNOWN_SERVER_ERROR,
                                            Collections.emptyList()));
                                } else {
                                    partitionData.complete(new ListOffsetResponse
                                            .PartitionData(
                                            Errors.UNKNOWN_SERVER_ERROR,
                                            ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                            ListOffsetResponse.UNKNOWN_OFFSET));
                                }
                                return;
                            }
                        } else {
                            finalPosition = (PositionImpl) position;
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("Find position for topic {} time {}. position: {}",
                                perTopic.getName(), timestamp, finalPosition);
                        }
                        if (legacyMode) {
                            partitionData.complete(new ListOffsetResponse.PartitionData(
                                    Errors.NONE,
                                    Collections.singletonList(
                                            MessageIdUtils.getOffset(
                                                    finalPosition.getLedgerId(), finalPosition.getEntryId()))));
                        } else {
                            partitionData.complete(new ListOffsetResponse.PartitionData(
                                    Errors.NONE,
                                    DEFAULT_TIMESTAMP,
                                    MessageIdUtils.getOffset(finalPosition.getLedgerId(), finalPosition.getEntryId())));
                        }
                    }

                    @Override
                    public void findEntryFailed(ManagedLedgerException exception,
                                                Optional<Position> position, Object ctx) {
                        log.warn("Unable to find position for topic {} time {}. Exception:",
                            perTopic.getName(), timestamp, exception);
                        if (legacyMode) {
                            partitionData.complete(new ListOffsetResponse
                                    .PartitionData(
                                    Errors.UNKNOWN_SERVER_ERROR,
                                    Collections.emptyList()));
                        } else {
                            partitionData.complete(new ListOffsetResponse
                                    .PartitionData(
                                    Errors.UNKNOWN_SERVER_ERROR,
                                    ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                    ListOffsetResponse.UNKNOWN_OFFSET));
                        }
                        return;
                    }
                });
            }
        });

        return partitionData;
    }

    private void handleListOffsetRequestV1AndAbove(KafkaHeaderAndRequest listOffset,
                                                   CompletableFuture<AbstractResponse> resultFuture) {
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        Map<TopicPartition, CompletableFuture<ListOffsetResponse.PartitionData>> responseData = Maps.newHashMap();

        request.partitionTimestamps().entrySet().stream().forEach(tms -> {
            TopicPartition topic = tms.getKey();
            Long times = tms.getValue();
            CompletableFuture<ListOffsetResponse.PartitionData> partitionData;

            CompletableFuture<PersistentTopic> persistentTopic = topicManager.getTopic(KopTopic.toString(topic));
            partitionData = fetchOffsetForTimestamp(persistentTopic, times, false);

            responseData.put(topic, partitionData);
        });

        CompletableFuture
                .allOf(responseData.values().stream().toArray(CompletableFuture<?>[]::new))
                .whenComplete((ignore, ex) -> {
                    ListOffsetResponse response =
                            new ListOffsetResponse(CoreUtils.mapValue(responseData, future -> future.join()));

                    resultFuture.complete(response);
                });
    }

    // Some info can be found here
    // https://cfchou.github.io/blog/2015/04/23/a-closer-look-at-kafka-offsetrequest/ through web.archive.org
    private void handleListOffsetRequestV0(KafkaHeaderAndRequest listOffset,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        Map<TopicPartition, CompletableFuture<ListOffsetResponse.PartitionData>> responseData = Maps.newHashMap();

        // in v0, the iterator is offsetData,
        // in v1, the iterator is partitionTimestamps,
        if (log.isDebugEnabled()) {
            log.debug("received a v0 listOffset: {}", request.toString(true));
        }
        request.offsetData().entrySet().stream().forEach(tms -> {
            TopicPartition topic = tms.getKey();
            String fullPartitionName = KopTopic.toString(topic);
            Long times = tms.getValue().timestamp;
            CompletableFuture<ListOffsetResponse.PartitionData> partitionData;

            // num_num_offsets > 1 is not handled for now, returning an error
            if (tms.getValue().maxNumOffsets > 1) {
                log.warn("request is asking for multiples offsets for {}, not supported for now", fullPartitionName);
                partitionData = new CompletableFuture<>();
                partitionData.complete(new ListOffsetResponse
                        .PartitionData(
                        Errors.UNKNOWN_SERVER_ERROR,
                        Collections.singletonList(ListOffsetResponse.UNKNOWN_OFFSET)));
            }

            CompletableFuture<PersistentTopic> persistentTopic = topicManager.getTopic(fullPartitionName);
            partitionData = fetchOffsetForTimestamp(persistentTopic, times, true);

            responseData.put(topic, partitionData);
        });

        CompletableFuture
                .allOf(responseData.values().stream().toArray(CompletableFuture<?>[]::new))
                .whenComplete((ignore, ex) -> {
                    ListOffsetResponse response =
                            new ListOffsetResponse(CoreUtils.mapValue(responseData, future -> future.join()));

                    resultFuture.complete(response);
                });
    }

    // get offset from underline managedLedger
    protected void handleListOffsetRequest(KafkaHeaderAndRequest listOffset,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(listOffset.getRequest() instanceof ListOffsetRequest);
        // the only difference between v0 and v1 is the `max_num_offsets => INT32`
        // v0 is required because it is used by librdkafka
        if (listOffset.getHeader().apiVersion() == 0) {
            handleListOffsetRequestV0(listOffset, resultFuture);
        } else {
            handleListOffsetRequestV1AndAbove(listOffset, resultFuture);
        }
    }

    // For non exist topics handleOffsetCommitRequest return UNKNOWN_TOPIC_OR_PARTITION
    private Map<TopicPartition, Errors> nonExistingTopicErrors(OffsetCommitRequest request) {
        // TODO: in Kafka Metadata cache, all topics in the cluster is included, we should support it?
        //       we could get all the topic info by listTopic?
        //      https://github.com/streamnative/kop/issues/51
        return Maps.newHashMap();
//        return request.offsetData().entrySet().stream()
//                .filter(entry ->
//                    // filter not exist topics
//                    !topicManager.topicExists(pulsarTopicName(entry.getKey(), namespace).toString()))
//                .collect(Collectors.toMap(
//                    e -> e.getKey(),
//                    e -> Errors.UNKNOWN_TOPIC_OR_PARTITION
//                ));
    }

    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(offsetCommit.getRequest() instanceof OffsetCommitRequest);
        checkState(groupCoordinator != null,
            "Group Coordinator not started");

        OffsetCommitRequest request = (OffsetCommitRequest) offsetCommit.getRequest();

        Map<TopicPartition, Errors> nonExistingTopic = nonExistingTopicErrors(request);

        groupCoordinator.handleCommitOffsets(
            request.groupId(),
            request.memberId(),
            request.generationId(),
            CoreUtils.mapValue(
                request.offsetData().entrySet().stream()
                    .filter(entry -> !nonExistingTopic.containsKey(entry.getKey()))
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())),
                (partitionData) ->
                    OffsetAndMetadata.apply(partitionData.offset, partitionData.metadata, partitionData.timestamp)
            )
        ).thenAccept(offsetCommitResult -> {
            if (nonExistingTopic != null) {
                offsetCommitResult.putAll(nonExistingTopic);
            }
            OffsetCommitResponse response = new OffsetCommitResponse(offsetCommitResult);
            resultFuture.complete(response);
        });
    }

    protected void handleFetchRequest(KafkaHeaderAndRequest fetch,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(fetch.getRequest() instanceof FetchRequest);
        FetchRequest request = (FetchRequest) fetch.getRequest();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Request {} Fetch request. Size: {}. Each item: ",
                ctx.channel(), fetch.getHeader(), request.fetchData().size());

            request.fetchData().forEach((topic, data) -> {
                log.debug("  Fetch request topic:{} data:{}.",
                    topic, data.toString());
            });
        }

        MessageFetchContext fetchContext = MessageFetchContext.get(this);
        fetchContext.handleFetch(resultFuture, fetch);
    }

    protected void handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(joinGroup.getRequest() instanceof JoinGroupRequest);
        checkState(groupCoordinator != null,
            "Group Coordinator not started");

        JoinGroupRequest request = (JoinGroupRequest) joinGroup.getRequest();

        Map<String, byte[]> protocols = new HashMap<>();
        request.groupProtocols()
            .stream()
            .forEach(protocol -> protocols.put(protocol.name(), Utils.toArray(protocol.metadata())));
        groupCoordinator.handleJoinGroup(
            request.groupId(),
            request.memberId(),
            joinGroup.getHeader().clientId(),
            joinGroup.getClientHost(),
            request.rebalanceTimeout(),
            request.sessionTimeout(),
            request.protocolType(),
            protocols
        ).thenAccept(joinGroupResult -> {

            Map<String, ByteBuffer> members = new HashMap<>();
            joinGroupResult.getMembers().forEach((memberId, protocol) ->
                members.put(memberId, ByteBuffer.wrap(protocol)));

            JoinGroupResponse response = new JoinGroupResponse(
                joinGroupResult.getError(),
                joinGroupResult.getGenerationId(),
                joinGroupResult.getSubProtocol(),
                joinGroupResult.getMemberId(),
                joinGroupResult.getLeaderId(),
                members
            );

            if (log.isTraceEnabled()) {
                log.trace("Sending join group response {} for correlation id {} to client {}.",
                    response, joinGroup.getHeader().correlationId(), joinGroup.getHeader().clientId());
            }

            resultFuture.complete(response);
        });
    }

    protected void handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(syncGroup.getRequest() instanceof SyncGroupRequest);
        SyncGroupRequest request = (SyncGroupRequest) syncGroup.getRequest();

        groupCoordinator.handleSyncGroup(
            request.groupId(),
            request.generationId(),
            request.memberId(),
            CoreUtils.mapValue(
                request.groupAssignment(), Utils::toArray
            )
        ).thenAccept(syncGroupResult -> {
            SyncGroupResponse response = new SyncGroupResponse(
                syncGroupResult.getKey(),
                ByteBuffer.wrap(syncGroupResult.getValue())
            );

            resultFuture.complete(response);
        });
    }

    protected void handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(heartbeat.getRequest() instanceof HeartbeatRequest);
        HeartbeatRequest request = (HeartbeatRequest) heartbeat.getRequest();

        // let the coordinator to handle heartbeat
        groupCoordinator.handleHeartbeat(
            request.groupId(),
            request.memberId(),
            request.groupGenerationId()
        ).thenAccept(errors -> {
            HeartbeatResponse response = new HeartbeatResponse(errors);

            if (log.isTraceEnabled()) {
                log.trace("Sending heartbeat response {} for correlation id {} to client {}.",
                    response, heartbeat.getHeader().correlationId(), heartbeat.getHeader().clientId());
            }

            resultFuture.complete(response);
        });
    }

    @Override
    protected void handleLeaveGroupRequest(KafkaHeaderAndRequest leaveGroup,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(leaveGroup.getRequest() instanceof LeaveGroupRequest);
        LeaveGroupRequest request = (LeaveGroupRequest) leaveGroup.getRequest();

        // let the coordinator to handle heartbeat
        groupCoordinator.handleLeaveGroup(
            request.groupId(),
            request.memberId()
        ).thenAccept(errors -> {
            LeaveGroupResponse response = new LeaveGroupResponse(errors);

            resultFuture.complete(response);
        });
    }

    @Override
    protected void handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(describeGroup.getRequest() instanceof DescribeGroupsRequest);
        DescribeGroupsRequest request = (DescribeGroupsRequest) describeGroup.getRequest();

        // let the coordinator to handle heartbeat
        Map<String, GroupMetadata> groups = request.groupIds().stream()
            .map(groupId -> {
                KeyValue<Errors, GroupSummary> describeResult = groupCoordinator
                    .handleDescribeGroup(groupId);
                GroupSummary summary = describeResult.getValue();
                List<GroupMember> members = summary.members().stream()
                    .map(member -> {
                        ByteBuffer metadata = ByteBuffer.wrap(member.metadata());
                        ByteBuffer assignment = ByteBuffer.wrap(member.assignment());
                        return new GroupMember(
                            member.memberId(),
                            member.clientId(),
                            member.clientHost(),
                            metadata,
                            assignment
                        );
                    })
                    .collect(Collectors.toList());
                return new KeyValue<>(
                    groupId,
                    new GroupMetadata(
                        describeResult.getKey(),
                        summary.state(),
                        summary.protocolType(),
                        summary.protocol(),
                        members
                    )
                );
            })
            .collect(Collectors.toMap(
                kv -> kv.getKey(),
                kv -> kv.getValue()
            ));
        DescribeGroupsResponse response = new DescribeGroupsResponse(
            groups
        );
        resultFuture.complete(response);
    }

    @Override
    protected void handleListGroupsRequest(KafkaHeaderAndRequest listGroups,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(listGroups.getRequest() instanceof ListGroupsRequest);
        KeyValue<Errors, List<GroupOverview>> listResult = groupCoordinator.handleListGroups();
        ListGroupsResponse response = new ListGroupsResponse(
            listResult.getKey(),
            listResult.getValue().stream()
                .map(groupOverview -> new Group(groupOverview.groupId(), groupOverview.protocolType()))
                .collect(Collectors.toList())
        );

        resultFuture.complete(response);
    }

    @Override
    protected void handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteGroups.getRequest() instanceof DeleteGroupsRequest);
        DeleteGroupsRequest request = (DeleteGroupsRequest) deleteGroups.getRequest();

        Map<String, Errors> deleteResult = groupCoordinator.handleDeleteGroups(request.groups());
        DeleteGroupsResponse response = new DeleteGroupsResponse(
            deleteResult
        );
        resultFuture.complete(response);
    }

    @Override
    protected void handleSaslAuthenticate(KafkaHeaderAndRequest saslAuthenticate,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.complete(new SaslAuthenticateResponse(Errors.ILLEGAL_SASL_STATE,
                "SaslAuthenticate request received after successful authentication"));
    }

    @Override
    protected void handleSaslHandshake(KafkaHeaderAndRequest saslHandshake,
                                       CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.complete(new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE, Collections.emptySet()));
    }

    @Override
    protected void handleCreateTopics(KafkaHeaderAndRequest createTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(createTopics.getRequest() instanceof CreateTopicsRequest);
        CreateTopicsRequest request = (CreateTopicsRequest) createTopics.getRequest();

        final Map<String, ApiError> result = new HashMap<>();
        final Map<String, TopicDetails> validTopics = new HashMap<>();
        final Set<String> duplicateTopics = request.duplicateTopics();

        request.topics().forEach((topic, details) -> {
            if (!duplicateTopics.contains(topic)) {
                validTopics.put(topic, details);
            } else {
                final String errorMessage = "Create topics request from client `" + createTopics.getHeader().clientId()
                        + "` contains multiple entries for the following topics: " + duplicateTopics;
                result.put(topic, new ApiError(Errors.INVALID_REQUEST, errorMessage));
            }
        });

        if (validTopics.isEmpty()) {
            resultFuture.complete(new CreateTopicsResponse(result));
        } else {
            // TODO: handle request.validateOnly()
            adminManager.createTopicsAsync(validTopics, request.timeout()).thenApply(validResult -> {
                result.putAll(validResult);
                resultFuture.complete(new CreateTopicsResponse(result));
                return null;
            });
        }
    }

    protected void handleDescribeConfigs(KafkaHeaderAndRequest describeConfigs,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(describeConfigs.getRequest() instanceof DescribeConfigsRequest);
        DescribeConfigsRequest request = (DescribeConfigsRequest) describeConfigs.getRequest();

        adminManager.describeConfigsAsync(new ArrayList<>(request.resources()).stream()
                .collect(Collectors.toMap(
                        resource -> resource,
                        resource -> Optional.ofNullable(request.configNames(resource)).map(HashSet::new)
                ))
        ).thenApply(configResourceConfigMap -> {
            resultFuture.complete(new DescribeConfigsResponse(0, configResourceConfigMap));
            return null;
        });
    }

    private SaslHandshakeResponse checkSaslMechanism(String mechanism) {
        if (getKafkaConfig().getSaslAllowedMechanisms().contains(mechanism)) {
            return new SaslHandshakeResponse(Errors.NONE, getKafkaConfig().getSaslAllowedMechanisms());
        }
        return new SaslHandshakeResponse(Errors.UNSUPPORTED_SASL_MECHANISM, new HashSet<>());
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        ctx.close();
    }

    private CompletableFuture<Optional<String>>
    getProtocolDataToAdvertise(InetSocketAddress pulsarAddress,
                               TopicName topic) {

        CompletableFuture<Optional<String>> returnFuture = new CompletableFuture<>();

        if (pulsarAddress == null) {
            log.error("[{}] failed get pulsar address, returned null.", topic.toString());

            // getTopicBroker returns null. topic should be removed from LookupCache.
            topicManager.removeTopicManagerCache(topic.toString());

            returnFuture.complete(Optional.empty());
            return returnFuture;
        }

        if (log.isDebugEnabled()) {
            log.debug("Found broker for topic {} puslarAddress: {}",
                topic, pulsarAddress);
        }

        // get kop address from cache to prevent query zk each time.
        if (topicManager.KOP_ADDRESS_CACHE.containsKey(topic.toString())) {
            return topicManager.KOP_ADDRESS_CACHE.get(topic.toString());
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
                    return;
                }

                // Get a list of ServiceLookupData for each matchBroker.
                List<CompletableFuture<Optional<ServiceLookupData>>> list = matchBrokers.stream()
                    .map(matchBroker ->
                        zkCache.getDataAsync(
                            String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, matchBroker),
                            (Deserializer<ServiceLookupData>)
                                pulsarService.getLoadManager().get().getLoadReportDeserializer()))
                    .collect(Collectors.toList());

                FutureUtil.waitForAll(list)
                    .whenComplete((ignore, th) -> {
                            if (th != null) {
                                log.error("Error in getDataAsync() for {}", pulsarAddress, th);
                                returnFuture.complete(Optional.empty());
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
                                        topicManager.KOP_ADDRESS_CACHE.put(topic.toString(), returnFuture);
                                        returnFuture.complete(data.getProtocol(KafkaProtocolHandler.PROTOCOL_NAME));
                                        return;
                                    }
                                }
                            } catch (Exception e) {
                                log.error("Error in {} lookupFuture get: ", pulsarAddress, e);
                                returnFuture.complete(Optional.empty());
                                return;
                            }

                            // no matching lookup data in all matchBrokers.
                            log.error("Not able to search {} in all child of zk://loadbalance", pulsarAddress);
                            returnFuture.complete(Optional.empty());
                        }
                    );
            });
        return returnFuture;
    }

    private boolean isOffsetTopic(String topic) {
        String offsetsTopic = kafkaConfig.getKafkaMetadataTenant() + "/"
            + kafkaConfig.getKafkaMetadataNamespace()
            + "/" + Topic.GROUP_METADATA_TOPIC_NAME;

        return topic.contains(offsetsTopic);
    }

    private CompletableFuture<PartitionMetadata> findBroker(TopicName topic) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Handle Lookup for {}", ctx.channel(), topic);
        }
        CompletableFuture<PartitionMetadata> returnFuture = new CompletableFuture<>();

        topicManager.getTopicBroker(topic.toString())
            .thenCompose(pair -> getProtocolDataToAdvertise(pair, topic))
            .whenComplete((stringOptional, throwable) -> {
                if (!stringOptional.isPresent() || throwable != null) {
                    log.error("Not get advertise data for Kafka topic:{}. throwable",
                        topic, throwable);
                    returnFuture.complete(null);
                    return;
                }

                String listeners = stringOptional.get();
                String kopBrokerUrl = getKopBrokerUrl(listeners, tlsEnabled);
                URI kopUri;
                try {
                    kopUri = new URI(kopBrokerUrl);
                } catch (URISyntaxException e) {
                    log.error("[{}] findBroker for topic {}: Failed to translate URI {}. exception:",
                        ctx.channel(), topic.toString(), kopBrokerUrl, e);
                    returnFuture.complete(null);
                    return;
                }

                Node node = newNode(new InetSocketAddress(
                    kopUri.getHost(),
                    kopUri.getPort()));

                if (log.isDebugEnabled()) {
                    log.debug("Found broker localListeners: {} for topicName: {}, "
                            + "localListeners: {}, found Listeners: {}",
                        listeners, topic, localListeners, listeners);
                }

                // here we found topic broker: broker2, but this is in broker1,
                // how to clean the lookup cache?
                if (!localListeners.contains(kopBrokerUrl)) {
                    topicManager.removeTopicManagerCache(topic.toString());
                }

                if (!topicManager.topicExists(topic.toString())
                    && localListeners.contains(kopBrokerUrl)) {
                    topicManager.getTopic(topic.toString()).whenComplete((persistentTopic, exception) -> {
                        if (exception != null || persistentTopic == null) {
                            log.warn("[{}] findBroker: Failed to getOrCreateTopic {}. broker:{}, exception:",
                                ctx.channel(), topic.toString(), kopBrokerUrl, exception);
                            returnFuture.complete(null);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Add topic: {} into TopicManager while findBroker.",
                                    topic.toString());
                            }
                            returnFuture.complete(newPartitionMetadata(topic, node));
                        }
                    });
                } else {
                    returnFuture.complete(newPartitionMetadata(topic, node));
                }
            });
        return returnFuture;
    }

    static Node newNode(InetSocketAddress address) {
        if (log.isDebugEnabled()) {
            log.debug("Return Broker Node of {}. {}:{}", address, address.getHostString(), address.getPort());
        }
        return new Node(
            Murmur3_32Hash.getInstance().makeHash((address.getHostString() + address.getPort()).getBytes(UTF_8)),
            address.getHostString(),
            address.getPort());
    }

    Node newSelfNode() {
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(
            kafkaConfig.getAdvertisedAddress());

        int port = tlsEnabled ? sslPort : plaintextPort;

        if (log.isDebugEnabled()) {
            log.debug("Return Broker Node of Self: {}:{}", hostname, port);
        }

        return new Node(
            Murmur3_32Hash.getInstance().makeHash((hostname + port).getBytes(UTF_8)),
            hostname,
            port);
    }


    static PartitionMetadata newPartitionMetadata(TopicName topicName, Node node) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        if (log.isDebugEnabled()) {
            log.debug("Return PartitionMetadata node: {}, topicName: {}", node, topicName);
        }

        return new PartitionMetadata(
            Errors.NONE,
            kafkaPartitionIndex,
            node,                      // leader
            Lists.newArrayList(node),  // replicas
            Lists.newArrayList(node),  // isr
            Collections.emptyList()     // offline replicas
        );
    }

    static PartitionMetadata newFailedPartitionMetadata(TopicName topicName) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        log.warn("Failed find Broker metadata, create PartitionMetadata with NOT_LEADER_FOR_PARTITION");

        // most of this error happens when topic is in loading/unloading status,
        return new PartitionMetadata(
            Errors.NOT_LEADER_FOR_PARTITION,
            kafkaPartitionIndex,
            Node.noNode(),                      // leader
            Lists.newArrayList(Node.noNode()),  // replicas
            Lists.newArrayList(Node.noNode()),  // isr
            Collections.emptyList()             // offline replicas
        );
    }

    static AbstractResponse failedResponse(KafkaHeaderAndRequest requestHar, Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("Request {} get failed response ", requestHar.getHeader().apiKey(), e);
        }
        return requestHar.getRequest().getErrorResponse(((Integer) THROTTLE_TIME_MS.defaultValue), e);
    }

    // whether a ServiceLookupData contains wanted address.
    static boolean lookupDataContainsAddress(ServiceLookupData data, String hostAndPort) {
        return (data.getPulsarServiceUrl() != null && data.getPulsarServiceUrl().contains(hostAndPort))
            || (data.getPulsarServiceUrlTls() != null && data.getPulsarServiceUrlTls().contains(hostAndPort))
            || (data.getWebServiceUrl() != null && data.getWebServiceUrl().contains(hostAndPort))
            || (data.getWebServiceUrlTls() != null && data.getWebServiceUrlTls().contains(hostAndPort));
    }
}
