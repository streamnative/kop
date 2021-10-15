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
import static io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration.TENANT_ALLNAMESPACES_PLACEHOLDER;
import static io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration.TENANT_PLACEHOLDER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.BYTES_IN;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_IN;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PARTITION_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.TOPIC_SCOPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupOverview;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupSummary;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.AbortedIndexEntry;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPTopicException;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatterFactory;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.offset.OffsetMetadata;
import io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator;
import io.streamnative.pulsar.handlers.kop.security.Session;
import io.streamnative.pulsar.handlers.kop.security.auth.Authorizer;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.security.auth.SimpleAclAuthorizer;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import io.streamnative.pulsar.handlers.kop.utils.OffsetFinder;
import io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils;
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperUtils;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMember;
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMetadata;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
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
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

/**
 * This class contains all the request handling methods.
 */
@Slf4j
@Getter
public class KafkaRequestHandler extends KafkaCommandDecoder {
    public static final long DEFAULT_TIMESTAMP = 0L;
    private static final String POLICY_ROOT = "/admin/policies/";

    private final PulsarService pulsarService;
    private final KafkaTopicManager topicManager;
    private final TenantContextManager tenantContextManager;

    private final String clusterName;
    private final ScheduledExecutorService executor;
    private final PulsarAdmin admin;
    private final SaslAuthenticator authenticator;
    private final Authorizer authorizer;
    private final AdminManager adminManager;
    private final MetadataCache<LocalBrokerData> localBrokerDataCache;

    private final Boolean tlsEnabled;
    private final EndPoint advertisedEndPoint;
    private final String advertisedListeners;
    private final int defaultNumPartitions;
    public final int maxReadEntriesNum;
    private final int failedAuthenticationDelayMs;
    // store the group name for current connected client.
    private final ConcurrentHashMap<String, String> currentConnectedGroup;
    private final String groupIdStoredPath;
    @Getter
    private final EntryFormatter entryFormatter;

    private final Set<String> groupIds = new HashSet<>();
    // key is the topic(partition), value is the future that indicates whether the PersistentTopic instance of the key
    // is found.
    private final Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap = new ConcurrentHashMap<>();
    // DelayedOperation for produce and fetch
    private final DelayedOperationPurgatory<DelayedOperation> producePurgatory =
            DelayedOperationPurgatory.<DelayedOperation>builder()
                    .purgatoryName("produce")
                    .timeoutTimer(SystemTimer.builder().executorName("produce").build())
                    .build();
    private final DelayedOperationPurgatory<DelayedOperation> fetchPurgatory =
            DelayedOperationPurgatory.<DelayedOperation>builder()
                    .purgatoryName("fetch")
                    .timeoutTimer(SystemTimer.builder().executorName("fetch").build())
                    .build();

    // Flag to manage throttling-publish-buffer by atomically enable/disable read-channel.
    private final long maxPendingBytes;
    private final long resumeThresholdPendingBytes;
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private volatile boolean autoReadDisabledPublishBufferLimiting = false;

    private String getCurrentTenant() {
        return getCurrentTenant(kafkaConfig.getKafkaMetadataTenant());
    }

    private String getCurrentTenant(String defaultTenant) {
        if (kafkaConfig.isKafkaEnableMultiTenantMetadata()
                && authenticator != null
                && authenticator.session() != null
                && authenticator.session().getPrincipal() != null
                && authenticator.session().getPrincipal().getTenantSpec() != null) {
            String tenantSpec =  authenticator.session().getPrincipal().getTenantSpec();
            return extractTenantFromTenantSpec(tenantSpec);
        }
        // fallback to using system (default) tenant
        if (log.isDebugEnabled()) {
            log.debug("using {} as tenant", defaultTenant);
        }
        return defaultTenant;
    }

    private static String extractTenantFromTenantSpec(String tenantSpec) {
        if (tenantSpec != null && !tenantSpec.isEmpty()) {
            String tenant = tenantSpec;
            // username can be "tenant" or "tenant/namespace"
            if (tenantSpec.contains("/")) {
                tenant = tenantSpec.substring(0, tenantSpec.indexOf('/'));
            }
            if (log.isDebugEnabled()) {
                log.debug("using {} as tenant", tenant);
            }
            return tenant;
        } else {
            return tenantSpec;
        }
    }

    public GroupCoordinator getGroupCoordinator() {
        return tenantContextManager.getGroupCoordinator(getCurrentTenant());
    }

    public TransactionCoordinator getTransactionCoordinator() {
        return tenantContextManager.getTransactionCoordinator(getCurrentTenant());
    }

    public KafkaRequestHandler(PulsarService pulsarService,
                               KafkaServiceConfiguration kafkaConfig,
                               TenantContextManager tenantContextManager,
                               AdminManager adminManager,
                               MetadataCache<LocalBrokerData> localBrokerDataCache,
                               Boolean tlsEnabled,
                               EndPoint advertisedEndPoint,
                               StatsLogger statsLogger) throws Exception {
        super(statsLogger, kafkaConfig);
        this.pulsarService = pulsarService;
        this.tenantContextManager = tenantContextManager;
        this.clusterName = kafkaConfig.getClusterName();
        this.executor = pulsarService.getExecutor();
        this.admin = pulsarService.getAdminClient();
        final boolean authenticationEnabled = pulsarService.getBrokerService().isAuthenticationEnabled()
                && !kafkaConfig.getSaslAllowedMechanisms().isEmpty();
        this.authenticator = authenticationEnabled
                ? new SaslAuthenticator(pulsarService, kafkaConfig.getSaslAllowedMechanisms(), kafkaConfig)
                : null;
        final boolean authorizationEnabled = pulsarService.getBrokerService().isAuthorizationEnabled();
        this.authorizer = authorizationEnabled && authenticationEnabled
                ? new SimpleAclAuthorizer(pulsarService)
                : null;
        this.adminManager = adminManager;
        this.localBrokerDataCache = localBrokerDataCache;
        this.tlsEnabled = tlsEnabled;
        this.advertisedEndPoint = advertisedEndPoint;
        this.advertisedListeners = kafkaConfig.getKafkaAdvertisedListeners();
        this.topicManager = new KafkaTopicManager(this);
        this.defaultNumPartitions = kafkaConfig.getDefaultNumPartitions();
        this.maxReadEntriesNum = kafkaConfig.getMaxReadEntriesNum();
        this.entryFormatter = EntryFormatterFactory.create(kafkaConfig.getEntryFormat());
        this.currentConnectedGroup = new ConcurrentHashMap<>();
        this.groupIdStoredPath = kafkaConfig.getGroupIdZooKeeperPath();
        this.maxPendingBytes = kafkaConfig.getMaxMessagePublishBufferSizeInMB() * 1024L * 1024L;
        this.resumeThresholdPendingBytes = this.maxPendingBytes / 2;
        this.failedAuthenticationDelayMs = kafkaConfig.getFailedAuthenticationDelayMs();

        // update alive channel count stats
        RequestStats.ALIVE_CHANNEL_COUNT_INSTANCE.incrementAndGet();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        topicManager.setRemoteAddress(ctx.channel().remoteAddress());
        if (authenticator != null) {
            authenticator.reset();
        }

        // update active channel count stats
        RequestStats.ACTIVE_CHANNEL_COUNT_INSTANCE.incrementAndGet();
        log.info("channel active: {}", ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        // update active channel count stats
        RequestStats.ACTIVE_CHANNEL_COUNT_INSTANCE.decrementAndGet();

        close();
    }

    @Override
    protected void close() {
        if (isActive.getAndSet(false)) {
            super.close();
            topicManager.close();
            String clientHost = ctx.channel().remoteAddress().toString();
            if (currentConnectedGroup.containsKey(clientHost)){
                log.info("currentConnectedGroup remove {}", clientHost);
                currentConnectedGroup.remove(clientHost);
            }
            producePurgatory.shutdown();
            fetchPurgatory.shutdown();

            // update alive channel count stat
            RequestStats.ALIVE_CHANNEL_COUNT_INSTANCE.decrementAndGet();
        }
    }

    @Override
    protected boolean hasAuthenticated() {
        return authenticator == null || authenticator.complete();
    }

    @Override
    protected void channelPrepare(ChannelHandlerContext ctx,
                                  ByteBuf requestBuf,
                                  BiConsumer<Long, Throwable> registerRequestParseLatency,
                                  BiConsumer<String, Long> registerRequestLatency)
            throws AuthenticationException {
        if (authenticator != null) {
            authenticator.authenticate(ctx, requestBuf, registerRequestParseLatency, registerRequestLatency,
                    this::validateTenantAccessForSession);
        }
    }

    @Override
    protected void maybeDelayCloseOnAuthenticationFailure() {
        if (this.failedAuthenticationDelayMs > 0) {
            this.ctx.executor().schedule(
                    this::handleCloseOnAuthenticationFailure,
                    this.failedAuthenticationDelayMs,
                    TimeUnit.MILLISECONDS);
        } else {
            handleCloseOnAuthenticationFailure();
        }
    }

    private void handleCloseOnAuthenticationFailure() {
        try {
            this.completeCloseOnAuthenticationFailure();
        } finally {
            this.close();
        }
    }

    @Override
    protected void completeCloseOnAuthenticationFailure() {
        if (isActive.get() && authenticator != null) {
            authenticator.sendAuthenticationFailureResponse();
        }
    }

    @Override
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

    @Override
    protected void handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                               CompletableFuture<AbstractResponse> resultFuture) {
        String err = String.format("Kafka API (%s) Not supported by kop server.",
            kafkaHeaderAndRequest.getHeader().apiKey());
        log.error(err);

        AbstractResponse apiResponse = kafkaHeaderAndRequest.getRequest()
            .getErrorResponse(new UnsupportedOperationException(err));
        resultFuture.complete(apiResponse);
    }

    @Override
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

    private static boolean isInternalTopic(final String fullTopicName) {
        String partitionedTopicName = TopicName.get(fullTopicName).getPartitionedTopicName();
        return partitionedTopicName.endsWith("/" + GROUP_METADATA_TOPIC_NAME)
                || partitionedTopicName.endsWith("/" + TRANSACTION_STATE_TOPIC_NAME);
    }

    private static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append(POLICY_ROOT);
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    private CompletableFuture<Set<String>> expandAllowedNamespaces(Set<String> allowedNamespaces) {
        String currentTenant = getCurrentTenant(kafkaConfig.getKafkaTenant());
        return expandAllowedNamespaces(allowedNamespaces, currentTenant, pulsarService);
    }

    @VisibleForTesting
    static CompletableFuture<Set<String>> expandAllowedNamespaces(Set<String> allowedNamespaces,
                                                                  String currentTenant,
                                                                  PulsarService pulsarService) {
        Set<String> result = new CopyOnWriteArraySet<>();
        List<CompletableFuture<?>> results = new ArrayList<>();
        for (String namespaceTemplate : allowedNamespaces) {
            String namespace = namespaceTemplate.replace(TENANT_PLACEHOLDER, currentTenant);
            if (!namespace.endsWith("/" + TENANT_ALLNAMESPACES_PLACEHOLDER)) {
                result.add(namespace);
                results.add(CompletableFuture.completedFuture(namespace));
            } else {
                int slash = namespace.lastIndexOf('/');
                String tenant = namespace.substring(0, slash);
                results.add(pulsarService.getPulsarResources()
                        .getNamespaceResources()
                        .getChildrenAsync(path(tenant))
                        .thenAccept(children -> {
                            children.forEach(ns -> {
                                result.add(tenant + "/" + ns);
                            });
                        }));
            }
        }
        return CompletableFuture
                .allOf(results.toArray(new CompletableFuture<?>[0]))
                .thenApply(f -> result);
    }

    // Get all topics in the configured allowed namespaces.
    //   key: the full topic name without partition suffix, e.g. persistent://public/default/my-topic
    //   value: the partitions associated with the key, e.g. for a topic with 3 partitions,
    //     persistent://public/default/my-topic-partition-0
    //     persistent://public/default/my-topic-partition-1
    //     persistent://public/default/my-topic-partition-2
    private CompletableFuture<Map<String, List<TopicName>>> getAllTopicsAsync() {
        CompletableFuture<Map<String, List<TopicName>>> topicMapFuture = new CompletableFuture<>();
        final Map<String, List<TopicName>> topicMap = new ConcurrentHashMap<>();
        final CompletableFuture<Set<String>> allowedNamespacesFuture =
                expandAllowedNamespaces(kafkaConfig.getKopAllowedNamespaces());

        allowedNamespacesFuture.thenAccept(allowedNamespaces -> {
            final AtomicInteger pendingNamespacesCount = new AtomicInteger(allowedNamespaces.size());

            for (String namespace : allowedNamespaces) {
                pulsarService.getNamespaceService().getListOfPersistentTopics(NamespaceName.get(namespace))
                        .whenComplete((topics, e) -> {
                            if (e != null) {
                                log.error("Failed to getListOfPersistentTopic of {}", namespace, e);
                                topicMapFuture.completeExceptionally(e);
                                return;
                            }
                            if (topicMapFuture.isCompletedExceptionally()) {
                                return;
                            }
                            for (String topic : topics) {
                                final TopicName topicName = TopicName.get(topic);
                                final String key = topicName.getPartitionedTopicName();
                                topicMap.computeIfAbsent(
                                        KopTopic.removeDefaultNamespacePrefix(key),
                                        ignored -> Collections.synchronizedList(new ArrayList<>())
                                ).add(topicName);
                            }
                            if (pendingNamespacesCount.decrementAndGet() == 0) {
                                topicMapFuture.complete(topicMap);
                            }
                        });
            }
        }).exceptionally(error -> {
            topicMapFuture.completeExceptionally(error);
            return null;
        });
        return topicMapFuture;
    }

    @Override
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
        // Get all kop brokers in local cache
        allNodes.addAll(adminManager.getBrokers(advertisedEndPoint.getListenerName()));

        List<String> topics = metadataRequest.topics();
        // topics in format : persistent://%s/%s/abc-partition-x, will be grouped by as:
        //      Entry<abc, List[TopicName]>

        // A future for a map from <kafka topic> to <pulsarPartitionTopics>:
        //      e.g. <topic1, {persistent://public/default/topic1-partition-0,...}>
        //   1. no topics provided, get all topics from namespace;
        //   2. topics provided, get provided topics.
        CompletableFuture<Map<String, List<TopicName>>> pulsarTopicsFuture;

        // Map for <partition-zero, non-partitioned-topic>, use for findBroker
        // e.g. <persistent://public/default/topic1-partition-0, persistent://public/default/topic1>
        final Map<String, TopicName> nonPartitionedTopicMap = Maps.newConcurrentMap();

        if (topics == null || topics.isEmpty()) {
            // clean all cache when get all metadata for librdkafka(<1.0.0).
            KafkaTopicManager.clearTopicManagerCache();
            // get all topics, filter by permissions.
            pulsarTopicsFuture = getAllTopicsAsync().thenApply((allTopicMap) -> {
                final Map<String, List<TopicName>> topicMap = new ConcurrentHashMap<>();
                allTopicMap.forEach((topic, list) -> {
                   list.forEach((topicName ->
                           authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, topicName.toString()))
                           .whenComplete((authorized, ex) -> {
                               if (ex != null || !authorized) {
                                   allTopicMetadata.add(new TopicMetadata(
                                           Errors.TOPIC_AUTHORIZATION_FAILED,
                                           topic,
                                           isInternalTopic(topicName.toString()),
                                           Collections.emptyList()));
                                   return;
                               }
                               topicMap.computeIfAbsent(
                                       topic,
                                       ignored -> Collections.synchronizedList(new ArrayList<>())
                               ).add(topicName);
                           })));
                });

                return topicMap;
            });
        } else {
            pulsarTopicsFuture = new CompletableFuture<>();
            // get only the provided topics
            final Map<String, List<TopicName>> pulsarTopics = Maps.newConcurrentMap();

            List<String> requestTopics = metadataRequest.topics();
            final int topicsNumber = requestTopics.size();
            AtomicInteger topicsCompleted = new AtomicInteger(0);

            final Runnable completeOneTopic = () -> {
                if (topicsCompleted.incrementAndGet() == topicsNumber) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Request {}: Completed get {} topic's partitions",
                                ctx.channel(), metadataHar.getHeader(), topicsNumber);
                    }
                    pulsarTopicsFuture.complete(pulsarTopics);
                }
            };

            final BiConsumer<String, Integer> addTopicPartition = (topic, partition) -> {
                final KopTopic kopTopic = new KopTopic(topic);
                pulsarTopics.putIfAbsent(topic,
                        IntStream.range(0, partition)
                                .mapToObj(i -> TopicName.get(kopTopic.getPartitionName(i)))
                                .collect(Collectors.toList()));
                completeOneTopic.run();
            };

            final BiConsumer<String, String> completeOneAuthFailedTopic = (topic, fullTopicName) -> {
                allTopicMetadata.add(new TopicMetadata(
                        Errors.TOPIC_AUTHORIZATION_FAILED,
                        topic,
                        isInternalTopic(fullTopicName),
                        Collections.emptyList()));
                completeOneTopic.run();
            };

            requestTopics.forEach(topic -> {
                final String fullTopicName = new KopTopic(topic).getFullName();

                authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, fullTopicName))
                    .whenComplete((authorized, ex) -> {
                        if (ex != null) {
                            log.error("Describe topic authorize failed, topic - {}. {}",
                                    fullTopicName, ex.getMessage());
                            // Authentication failed
                            completeOneAuthFailedTopic.accept(topic, fullTopicName);
                            return;
                        }
                        if (!authorized) {
                            // Permission denied
                            completeOneAuthFailedTopic.accept(topic, fullTopicName);
                            return;
                        }
                        // get partition numbers for each topic.
                        // If topic doesn't exist and allowAutoTopicCreation is enabled,
                        // the topic will be created first.
                        getPartitionedTopicMetadataAsync(fullTopicName)
                                .whenComplete((partitionedTopicMetadata, throwable) -> {
                                    if (throwable != null) {
                                        if (throwable instanceof PulsarAdminException.NotFoundException) {
                                            if (kafkaConfig.isAllowAutoTopicCreation()
                                                    && metadataRequest.allowAutoTopicCreation()) {
                                                log.info("[{}] Request {}: Topic {} doesn't exist, "
                                                                + "auto create it with {} partitions",
                                                        ctx.channel(), metadataHar.getHeader(),
                                                        topic, defaultNumPartitions);
                                                admin.topics().createPartitionedTopicAsync(
                                                                fullTopicName, defaultNumPartitions)
                                                        .whenComplete((ignored, e) -> {
                                                            if (e == null) {
                                                                addTopicPartition.accept(topic, defaultNumPartitions);
                                                            } else {
                                                                log.error("[{}] Failed to create partitioned topic {}",
                                                                        ctx.channel(), topic, e);
                                                                completeOneTopic.run();
                                                            }
                                                        });
                                            } else {
                                                log.error("[{}] Request {}: Topic {} doesn't exist and it's "
                                                                + "not allowed to auto create partitioned topic",
                                                        ctx.channel(), metadataHar.getHeader(), topic);
                                                // not allow to auto create topic, return unknown topic
                                                allTopicMetadata.add(
                                                        new TopicMetadata(
                                                                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                                topic,
                                                                isInternalTopic(fullTopicName),
                                                                Collections.emptyList()));
                                                completeOneTopic.run();
                                            }
                                        } else {
                                            // Failed get partitions.
                                            allTopicMetadata.add(
                                                    new TopicMetadata(
                                                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                            topic,
                                                            isInternalTopic(fullTopicName),
                                                            Collections.emptyList()));
                                            log.warn("[{}] Request {}: Failed to get partitioned pulsar topic {} "
                                                            + "metadata: {}",
                                                    ctx.channel(), metadataHar.getHeader(),
                                                    fullTopicName, throwable.getMessage());
                                            completeOneTopic.run();
                                        }
                                    } else { // the topic already existed
                                        if (partitionedTopicMetadata.partitions > 0) {
                                            if (log.isDebugEnabled()) {
                                                log.debug("Topic {} has {} partitions",
                                                        topic, partitionedTopicMetadata.partitions);
                                            }
                                            addTopicPartition.accept(topic, partitionedTopicMetadata.partitions);
                                        } else {
                                            // In case non-partitioned topic, treat as a one partitioned topic.
                                            nonPartitionedTopicMap.put(TopicName
                                                            .get(fullTopicName)
                                                            .getPartition(0)
                                                            .toString(),
                                                    TopicName.get(fullTopicName)
                                            );
                                            addTopicPartition.accept(topic, 1);
                                        }
                                    }
                                });
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

                list.forEach(topicName -> {
                    // For non-partitioned topic.
                    TopicName realTopicName = nonPartitionedTopicMap.getOrDefault(topicName.toString(), topicName);
                    findBroker(realTopicName)
                            .whenComplete(((partitionMetadata, throwable) -> {
                                if (throwable != null || partitionMetadata == null) {
                                    log.warn("[{}] Request {}: Exception while find Broker metadata",
                                            ctx.channel(), metadataHar.getHeader(), throwable);
                                    partitionMetadatas.add(newFailedPartitionMetadata(realTopicName));
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
                                                    // The topic returned to Kafka clients should be
                                                    // the same with what it sent
                                                    topic,
                                                    isInternalTopic(new KopTopic(topic).getFullName()),
                                                    partitionMetadatas));

                                    // whether completed all the topics requests.
                                    int finishedTopics = topicsCompleted.incrementAndGet();
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Request {}: Completed findBroker for topic {}, "
                                                        + "partitions found/all: {}/{}. \n dump All Metadata:",
                                                ctx.channel(), metadataHar.getHeader(), topic,
                                                finishedTopics, topicsNumber);

                                        allTopicMetadata.stream()
                                                .forEach(data -> log.debug("TopicMetadata response: {}",
                                                        data.toString()));
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
                            }));
                });
            });
        });
    }

    private void disableCnxAutoRead() {
        if (ctx != null && ctx.channel().config().isAutoRead()) {
            ctx.channel().config().setAutoRead(false);
            if (log.isDebugEnabled()) {
                log.debug("[{}] disable auto read", ctx.channel());
            }
        }
    }

    private void enableCnxAutoRead() {
        if (ctx != null && !ctx.channel().config().isAutoRead()
                && !autoReadDisabledPublishBufferLimiting) {
            // Resume reading from socket if pending-request is not reached to threshold
            ctx.channel().config().setAutoRead(true);
            // triggers channel read
            ctx.read();
            if (log.isDebugEnabled()) {
                log.debug("[{}] enable auto read", ctx.channel());
            }
        }
    }

    private void startSendOperationForThrottling(long msgSize) {
        final long currentPendingBytes = pendingBytes.addAndGet(msgSize);
        if (currentPendingBytes >= maxPendingBytes && !autoReadDisabledPublishBufferLimiting && maxPendingBytes > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] disable auto read because currentPendingBytes({}) > maxPendingBytes({})",
                        ctx.channel(), currentPendingBytes, maxPendingBytes);
            }
            disableCnxAutoRead();
            autoReadDisabledPublishBufferLimiting = true;
            pulsarService.getBrokerService().pausedConnections(1);
        }
    }

    private void completeSendOperationForThrottling(long msgSize) {
        final long currentPendingBytes = pendingBytes.addAndGet(-msgSize);
        if (currentPendingBytes < resumeThresholdPendingBytes && autoReadDisabledPublishBufferLimiting) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] enable auto read because currentPendingBytes({}) < resumeThreshold({})",
                        ctx.channel(), currentPendingBytes, resumeThresholdPendingBytes);
            }
            autoReadDisabledPublishBufferLimiting = false;
            enableCnxAutoRead();
            pulsarService.getBrokerService().resumedConnections(1);
        }
    }

    private void publishMessages(final Optional<PersistentTopic> persistentTopicOpt,
                                 final ByteBuf byteBuf,
                                 final int numMessages,
                                 final MemoryRecords records,
                                 final TopicPartition topicPartition,
                                 final Consumer<Long> offsetConsumer,
                                 final Consumer<Errors> errorsConsumer) {
        if (!persistentTopicOpt.isPresent()) {
            // It will trigger a retry send of Kafka client
            errorsConsumer.accept(Errors.NOT_LEADER_FOR_PARTITION);
            return;
        }
        PersistentTopic persistentTopic = persistentTopicOpt.get();
        if (persistentTopic.isSystemTopic()) {
            log.error("Not support producing message to system topic: {}", persistentTopic);
            errorsConsumer.accept(Errors.INVALID_TOPIC_EXCEPTION);
            return;
        }
        final String partitionName = KopTopic.toString(topicPartition);

        topicManager.registerProducerInPersistentTopic(partitionName, persistentTopic);
        // collect metrics
        final Producer producer = KafkaTopicManager.getReferenceProducer(partitionName);
        producer.updateRates(numMessages, byteBuf.readableBytes());
        producer.getTopic().incrementPublishCount(numMessages, byteBuf.readableBytes());
        updateProducerStats(topicPartition, numMessages, byteBuf.readableBytes());

        // publish
        final CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        final long beforePublish = MathUtils.nowInNano();
        persistentTopic.publishMessage(byteBuf,
                MessagePublishContext.get(offsetFuture, persistentTopic, numMessages, System.nanoTime()));
        final RecordBatch batch = records.batchIterator().next();
        offsetFuture.whenComplete((offset, e) -> {
            completeSendOperationForThrottling(byteBuf.readableBytes());
            byteBuf.release();
            if (e == null) {
                if (batch.isTransactional()) {
                    getTransactionCoordinator().addActivePidOffset(TopicName.get(partitionName), batch.producerId(),
                                                                    offset);
                }
                requestStats.getMessagePublishStats().registerSuccessfulEvent(
                        MathUtils.elapsedNanos(beforePublish), TimeUnit.NANOSECONDS);
                offsetConsumer.accept(offset);
            } else {
                log.error("publishMessages for topic partition: {} failed when write.", partitionName, e);
                requestStats.getMessagePublishStats().registerFailedEvent(
                        MathUtils.elapsedNanos(beforePublish), TimeUnit.NANOSECONDS);
                errorsConsumer.accept(Errors.KAFKA_STORAGE_ERROR);
            }
        });
    }

    @Override
    protected void handleProduceRequest(KafkaHeaderAndRequest produceHar,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(produceHar.getRequest() instanceof ProduceRequest);
        ProduceRequest produceRequest = (ProduceRequest) produceHar.getRequest();

        final int numPartitions = produceRequest.partitionRecordsOrFail().size();

        final Map<TopicPartition, PartitionResponse> responseMap = new ConcurrentHashMap<>();
        // delay produce
        final AtomicInteger topicPartitionNum = new AtomicInteger(produceRequest.partitionRecordsOrFail().size());
        int timeoutMs = produceRequest.timeout();
        Runnable complete = () -> {
            topicPartitionNum.set(0);
            if (resultFuture.isDone()) {
                // It may be triggered again in DelayedProduceAndFetch
                return;
            }
            // add the topicPartition with timeout error if it's not existed in responseMap
            produceRequest.partitionRecordsOrFail().keySet().forEach(topicPartition -> {
                if (!responseMap.containsKey(topicPartition)) {
                    responseMap.put(topicPartition, new PartitionResponse(Errors.REQUEST_TIMED_OUT));
                }
            });
            if (log.isDebugEnabled()) {
                log.debug("[{}] Request {}: Complete handle produce.", ctx.channel(), produceHar.toString());
            }
            resultFuture.complete(new ProduceResponse(responseMap));
        };
        BiConsumer<TopicPartition, PartitionResponse> addPartitionResponse = (topicPartition, response) -> {
            responseMap.put(topicPartition, response);
            // reset topicPartitionNum
            int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
            if (restTopicPartitionNum < 0) {
                return;
            }
            if (restTopicPartitionNum == 0) {
                complete.run();
            }
        };

        produceRequest.partitionRecordsOrFail().forEach((topicPartition, records) -> {
            final Consumer<Long> offsetConsumer = offset -> addPartitionResponse.accept(
                    topicPartition, new PartitionResponse(Errors.NONE, offset, -1L, -1L));
            final Consumer<Errors> errorsConsumer =
                    errors -> addPartitionResponse.accept(topicPartition, new PartitionResponse(errors));
            final Consumer<Throwable> exceptionConsumer =
                    e -> addPartitionResponse.accept(topicPartition, new PartitionResponse(Errors.forException(e)));
            final String fullPartitionName = KopTopic.toString(topicPartition);

            authorize(AclOperation.WRITE, Resource.of(ResourceType.TOPIC, fullPartitionName))
                    .whenComplete((isAuthorized, ex) -> {
                        if (ex != null) {
                            log.error("Write topic authorize failed, topic - {}. {}",
                                    fullPartitionName, ex.getMessage());
                            errorsConsumer.accept(Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }
                        if (!isAuthorized) {
                            errorsConsumer.accept(Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }

                        handlePartitionRecords(produceHar,
                                topicPartition,
                                records,
                                numPartitions,
                                fullPartitionName,
                                offsetConsumer,
                                errorsConsumer,
                                exceptionConsumer);
                    });
        });
        // delay produce
        if (timeoutMs <= 0) {
            complete.run();
        } else {
            List<Object> delayedCreateKeys =
                    produceRequest.partitionRecordsOrFail().keySet().stream()
                            .map(DelayedOperationKey.TopicPartitionOperationKey::new).collect(Collectors.toList());
            DelayedProduceAndFetch delayedProduce = new DelayedProduceAndFetch(timeoutMs, topicPartitionNum, complete);
            producePurgatory.tryCompleteElseWatch(delayedProduce, delayedCreateKeys);
        }
    }

    private void handlePartitionRecords(final KafkaHeaderAndRequest produceHar,
                                        final TopicPartition topicPartition,
                                        final MemoryRecords records,
                                        final int numPartitions,
                                        final String fullPartitionName,
                                        final Consumer<Long> offsetConsumer,
                                        final Consumer<Errors> errorsConsumer,
                                        final Consumer<Throwable> exceptionConsumer) {
        // check KOP inner topic
        if (isInternalTopic(fullPartitionName)) {
            log.error("[{}] Request {}: not support produce message to inner topic. topic: {}",
                    ctx.channel(), produceHar.getHeader(), topicPartition);
            errorsConsumer.accept(Errors.INVALID_TOPIC_EXCEPTION);
            return;
        }

        try {
            final long beforeRecordsProcess = MathUtils.nowInNano();
            final MemoryRecords validRecords =
                    validateRecords(produceHar.getHeader().apiVersion(), topicPartition, records);
            final int numMessages = EntryFormatter.parseNumMessages(validRecords);
            final ByteBuf byteBuf = entryFormatter.encode(validRecords, numMessages);
            requestStats.getProduceEncodeStats().registerSuccessfulEvent(
                    MathUtils.elapsedNanos(beforeRecordsProcess), TimeUnit.NANOSECONDS);
            startSendOperationForThrottling(byteBuf.readableBytes());

            if (log.isDebugEnabled()) {
                log.debug("[{}] Request {}: Produce messages for topic {} partition {}, "
                                + "request size: {} ", ctx.channel(), produceHar.getHeader(),
                        topicPartition.topic(), topicPartition.partition(), numPartitions);
            }

            final CompletableFuture<Optional<PersistentTopic>> topicFuture =
                    topicManager.getTopic(fullPartitionName);
            if (topicFuture.isCompletedExceptionally()) {
                topicFuture.exceptionally(e -> {
                    exceptionConsumer.accept(e);
                    return Optional.empty();
                });
                return;
            }
            if (topicFuture.isDone() && !topicFuture.getNow(Optional.empty()).isPresent()) {
                errorsConsumer.accept(Errors.NOT_LEADER_FOR_PARTITION);
                return;
            }

            final Consumer<Optional<PersistentTopic>> persistentTopicConsumer = persistentTopicOpt -> {
                publishMessages(persistentTopicOpt, byteBuf, numMessages, validRecords, topicPartition,
                        offsetConsumer, errorsConsumer);
            };

            if (topicFuture.isDone()) {
                persistentTopicConsumer.accept(topicFuture.getNow(Optional.empty()));
            } else {
                // topic is not available now
                pendingTopicFuturesMap
                        .computeIfAbsent(topicPartition, ignored ->
                                new PendingTopicFutures(requestStats))
                        .addListener(topicFuture, persistentTopicConsumer, exceptionConsumer);
            }
        } catch (Exception e) {
            log.error("[{}] Failed to handle produce request for {}",
                    ctx.channel(), topicPartition, e);
            exceptionConsumer.accept(e);
        }
    }

    @Override
    protected void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator,
                                                CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(findCoordinator.getRequest() instanceof FindCoordinatorRequest);
        FindCoordinatorRequest request = (FindCoordinatorRequest) findCoordinator.getRequest();

        String pulsarTopicName;
        int partition;

        if (request.coordinatorType() == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
            TransactionCoordinator transactionCoordinator = getTransactionCoordinator();
            partition = transactionCoordinator.partitionFor(request.coordinatorKey());
            pulsarTopicName = transactionCoordinator.getTopicPartitionName(partition);
        } else if (request.coordinatorType() == FindCoordinatorRequest.CoordinatorType.GROUP) {
            partition = getGroupCoordinator().partitionFor(request.coordinatorKey());
            pulsarTopicName = getGroupCoordinator().getTopicPartitionName(partition);
        } else {
            throw new NotImplementedException("FindCoordinatorRequest not support TRANSACTION type "
                + request.coordinatorType());
        }
        // store group name to zk for current client
        String groupId = request.coordinatorKey();
        String zkSubPath = ZooKeeperUtils.groupIdPathFormat(findCoordinator.getClientHost(),
                findCoordinator.getHeader().clientId());
        byte[] groupIdBytes = groupId.getBytes(Charset.forName("UTF-8"));
        ZooKeeperUtils.tryCreatePath(pulsarService.getZkClient(), groupIdStoredPath + zkSubPath, groupIdBytes);

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
    }

    @VisibleForTesting
    public <T> void replaceTopicPartition(Map<TopicPartition, T> replacedMap,
                                          Map<TopicPartition, TopicPartition> replacingIndex) {
        Map<TopicPartition, T> newMap = new HashMap<>();
        replacedMap.entrySet().removeIf(entry -> {
            if (replacingIndex.containsKey(entry.getKey())) {
                newMap.put(replacingIndex.get(entry.getKey()), entry.getValue());
                return true;
            } else if (KopTopic.isFullTopicName(entry.getKey().topic())) {
                newMap.put(new TopicPartition(
                                KopTopic.removeDefaultNamespacePrefix(entry.getKey().topic()),
                                entry.getKey().partition()),
                        entry.getValue());
                return true;
            }
            return false;
        });
        replacedMap.putAll(newMap);
    }

    @Override
    protected void handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(offsetFetch.getRequest() instanceof OffsetFetchRequest);
        OffsetFetchRequest request = (OffsetFetchRequest) offsetFetch.getRequest();
        checkState(getGroupCoordinator() != null,
            "Group Coordinator not started");

        CompletableFuture<List<TopicPartition>> authorizeFuture = new CompletableFuture<>();

        // replace
        Map<TopicPartition, TopicPartition> replacingIndex = new HashMap<>();

        List<TopicPartition> authorizedPartitions = new ArrayList<>();
        Map<TopicPartition, OffsetFetchResponse.PartitionData> unauthorizedPartitionData =
                Maps.newConcurrentMap();
        Map<TopicPartition, OffsetFetchResponse.PartitionData> unknownPartitionData =
                Maps.newConcurrentMap();

        if (request.partitions() == null || request.partitions().isEmpty()) {
            authorizeFuture.complete(null);
        } else {
            AtomicInteger partitionCount = new AtomicInteger(request.partitions().size());

            Runnable completeOneAuthorization = () -> {
                if (partitionCount.decrementAndGet() == 0) {
                    authorizeFuture.complete(authorizedPartitions);
                }
            };
            request.partitions().forEach(tp -> {
                try {
                    String fullName =  new KopTopic(tp.topic()).getFullName();
                    authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, fullName))
                            .whenComplete((isAuthorized, ex) -> {
                                if (ex != null) {
                                    log.error("Describe topic authorize failed, topic - {}. {}",
                                            fullName, ex.getMessage());
                                    unauthorizedPartitionData.put(tp, OffsetFetchResponse.UNAUTHORIZED_PARTITION);
                                    completeOneAuthorization.run();
                                    return;
                                }
                                if (!isAuthorized) {
                                    unauthorizedPartitionData.put(tp, OffsetFetchResponse.UNAUTHORIZED_PARTITION);
                                    completeOneAuthorization.run();
                                    return;
                                }
                                TopicPartition newTopicPartition = new TopicPartition(
                                        fullName, tp.partition());
                                replacingIndex.put(newTopicPartition, tp);
                                authorizedPartitions.add(newTopicPartition);
                                completeOneAuthorization.run();
                            });
                } catch (KoPTopicException e) {
                    log.warn("Invalid topic name: {}", tp.topic(), e);
                    unknownPartitionData.put(tp, OffsetFetchResponse.UNKNOWN_PARTITION);
                }
            });
        }

        authorizeFuture.whenComplete((partitionList, ex) -> {
            KeyValue<Errors, Map<TopicPartition, OffsetFetchResponse.PartitionData>> keyValue =
                    getGroupCoordinator().handleFetchOffsets(
                            request.groupId(),
                            Optional.ofNullable(partitionList)
                    );
            if (log.isDebugEnabled()) {
                log.debug("OFFSET_FETCH Unknown partitions: {}, Unauthorized partitions: {}.",
                        unknownPartitionData, unauthorizedPartitionData);
            }

            if (log.isTraceEnabled()) {
                StringBuffer traceInfo = new StringBuffer();
                replacingIndex.forEach((inner, outer) ->
                        traceInfo.append(String.format("\tinnerName:%s, outerName:%s%n", inner, outer)));
                log.trace("OFFSET_FETCH TopicPartition relations: \n{}", traceInfo);
            }

            // recover to original topic name
            replaceTopicPartition(keyValue.getValue(), replacingIndex);
            keyValue.getValue().putAll(unauthorizedPartitionData);
            keyValue.getValue().putAll(unknownPartitionData);

            resultFuture.complete(new OffsetFetchResponse(keyValue.getKey(), keyValue.getValue()));
        });
    }

    private CompletableFuture<ListOffsetResponse.PartitionData>
    fetchOffsetForTimestamp(String topicName, Long timestamp, boolean legacyMode) {
        CompletableFuture<ListOffsetResponse.PartitionData> partitionData = new CompletableFuture<>();

        topicManager.getTopic(topicName).whenComplete((perTopicOpt, t) -> {
            if (t != null) {
                log.error("Failed while get persistentTopic topic: {} ts: {}. ",
                    !perTopicOpt.isPresent() ? "null" : perTopicOpt.get().getName(), timestamp, t);
                partitionData.complete(new ListOffsetResponse.PartitionData(
                        Errors.forException(t),
                        ListOffsetResponse.UNKNOWN_TIMESTAMP,
                        ListOffsetResponse.UNKNOWN_OFFSET));
                return;
            }
            if (!perTopicOpt.isPresent()) {
                partitionData.complete(new ListOffsetResponse.PartitionData(
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        ListOffsetResponse.UNKNOWN_TIMESTAMP,
                        ListOffsetResponse.UNKNOWN_OFFSET));
                return;
            }
            PersistentTopic perTopic = perTopicOpt.get();
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) perTopic.getManagedLedger();
            PositionImpl lac = (PositionImpl) managedLedger.getLastConfirmedEntry();
            if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP) {
                PositionImpl position = (PositionImpl) managedLedger.getLastConfirmedEntry();
                if (log.isDebugEnabled()) {
                    log.debug("Get latest position for topic {} time {}. result: {}",
                        perTopic.getName(), timestamp, position);
                }
                long offset = MessageIdUtils.getLogEndOffset(managedLedger);
                fetchOffsetForTimestampSuccess(partitionData, legacyMode, offset);

            } else if (timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP) {
                PositionImpl position = OffsetFinder.getFirstValidPosition(managedLedger);

                if (log.isDebugEnabled()) {
                    log.debug("Get earliest position for topic {} time {}. result: {}",
                        perTopic.getName(), timestamp, position);
                }
                if (position.compareTo(lac) > 0 || MessageIdUtils.getCurrentOffset(managedLedger) < 0) {
                    long offset = Math.max(0, MessageIdUtils.getCurrentOffset(managedLedger));
                    fetchOffsetForTimestampSuccess(partitionData, legacyMode, offset);
                } else {
                    MessageIdUtils.getOffsetOfPosition(managedLedger, position, false, timestamp)
                            .whenComplete((offset, throwable) -> {
                                if (throwable != null) {
                                    log.error("[{}] Failed to get offset for position {}",
                                            perTopic, position, throwable);
                                    fetchOffsetForTimestampFailed(partitionData, legacyMode);
                                    return;
                                }
                                fetchOffsetForTimestampSuccess(partitionData, legacyMode, offset);
                    });
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
                                fetchOffsetForTimestampFailed(partitionData, legacyMode);
                                return;
                            }
                        } else {
                            finalPosition = (PositionImpl) position;
                        }


                        if (log.isDebugEnabled()) {
                            log.debug("Find position for topic {} time {}. position: {}",
                                perTopic.getName(), timestamp, finalPosition);
                        }

                        if (finalPosition.compareTo(lac) > 0 || MessageIdUtils.getCurrentOffset(managedLedger) < 0) {
                            long offset = Math.max(0, MessageIdUtils.getCurrentOffset(managedLedger));
                            fetchOffsetForTimestampSuccess(partitionData, legacyMode, offset);
                        } else {
                            MessageIdUtils.getOffsetOfPosition(managedLedger, finalPosition, true, timestamp)
                                    .whenComplete((offset, throwable) -> {
                                        if (throwable != null) {
                                            log.error("[{}] Failed to get offset for position {}",
                                                    perTopic, finalPosition, throwable);
                                            fetchOffsetForTimestampFailed(partitionData, legacyMode);
                                            return;
                                        }
                                        fetchOffsetForTimestampSuccess(partitionData, legacyMode, offset);
                                    });
                        }
                    }

                    @Override
                    public void findEntryFailed(ManagedLedgerException exception,
                                                Optional<Position> position, Object ctx) {
                        log.warn("Unable to find position for topic {} time {}. Exception:",
                            perTopic.getName(), timestamp, exception);
                        fetchOffsetForTimestampFailed(partitionData, legacyMode);
                    }
                });
            }
        });

        return partitionData;
    }

    private void fetchOffsetForTimestampFailed(CompletableFuture<ListOffsetResponse.PartitionData> partitionData,
                                               boolean legacyMode) {
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
    }

    private void fetchOffsetForTimestampSuccess(CompletableFuture<ListOffsetResponse.PartitionData> partitionData,
                                                boolean legacyMode,
                                                long offset) {
        if (legacyMode) {
            partitionData.complete(new ListOffsetResponse.PartitionData(
                    Errors.NONE,
                    Collections.singletonList(offset)));
        } else {
            partitionData.complete(new ListOffsetResponse.PartitionData(
                    Errors.NONE,
                    DEFAULT_TIMESTAMP,
                    offset));
        }
    }

    private void handleListOffsetRequestV1AndAbove(KafkaHeaderAndRequest listOffset,
                                                   CompletableFuture<AbstractResponse> resultFuture) {
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();
        Map<TopicPartition, CompletableFuture<ListOffsetResponse.PartitionData>> responseData =
                Maps.newConcurrentMap();

        request.partitionTimestamps().forEach((topic, times) -> {
            String fullPartitionName = KopTopic.toString(topic);
            authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, fullPartitionName))
                    .whenComplete((isAuthorized, ex) -> {
                                if (ex != null) {
                                    log.error("Describe topic authorize failed, topic - {}. {}",
                                            fullPartitionName, ex.getMessage());
                                    responseData.put(topic, CompletableFuture.completedFuture(new ListOffsetResponse
                                            .PartitionData(
                                            Errors.TOPIC_AUTHORIZATION_FAILED,
                                            Collections.emptyList()
                                    )));
                                    return;
                                }
                                if (!isAuthorized) {
                                    responseData.put(topic, CompletableFuture.completedFuture(new ListOffsetResponse
                                            .PartitionData(
                                            Errors.TOPIC_AUTHORIZATION_FAILED,
                                            Collections.emptyList()
                                    )));
                                    return;
                                }
                                responseData.put(topic,
                                        fetchOffsetForTimestamp(fullPartitionName, times, false));
                            }
                        );

        });

        CompletableFuture
                .allOf(responseData.values().toArray(new CompletableFuture<?>[0]))
                .whenComplete((ignore, ex) -> {
                    ListOffsetResponse response =
                            new ListOffsetResponse(CoreUtils.mapValue(responseData, CompletableFuture::join));
                    resultFuture.complete(response);
                });
    }

    // Some info can be found here
    // https://cfchou.github.io/blog/2015/04/23/a-closer-look-at-kafka-offsetrequest/ through web.archive.org
    private void handleListOffsetRequestV0(KafkaHeaderAndRequest listOffset,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        Map<TopicPartition, CompletableFuture<ListOffsetResponse.PartitionData>> responseData =
                Maps.newConcurrentMap();

        // in v0, the iterator is offsetData,
        // in v1, the iterator is partitionTimestamps,
        if (log.isDebugEnabled()) {
            log.debug("received a v0 listOffset: {}", request.toString(true));
        }
        request.offsetData().forEach((topic, value) -> {
            String fullPartitionName = KopTopic.toString(topic);

            authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, fullPartitionName))
                    .whenComplete((isAuthorized, ex) -> {
                        if (ex != null) {
                            log.error("Describe topic authorize failed, topic - {}. {}",
                                    fullPartitionName, ex.getMessage());
                            responseData.put(topic, CompletableFuture.completedFuture(new ListOffsetResponse
                                    .PartitionData(
                                    Errors.TOPIC_AUTHORIZATION_FAILED,
                                    Collections.emptyList())));
                            return;
                        }
                        if (!isAuthorized) {
                            responseData.put(topic, CompletableFuture.completedFuture(new ListOffsetResponse
                                    .PartitionData(
                                    Errors.TOPIC_AUTHORIZATION_FAILED,
                                    Collections.emptyList())));
                            return;
                        }
                        Long times = value.timestamp;

                        CompletableFuture<ListOffsetResponse.PartitionData> partitionData;
                        // num_num_offsets > 1 is not handled for now, returning an error
                        if (value.maxNumOffsets > 1) {
                            log.warn("request is asking for multiples offsets for {}, not supported for now",
                                    fullPartitionName);
                            partitionData = new CompletableFuture<>();
                            partitionData.complete(new ListOffsetResponse
                                    .PartitionData(
                                    Errors.UNKNOWN_SERVER_ERROR,
                                    Collections.singletonList(ListOffsetResponse.UNKNOWN_OFFSET)));
                        }

                        partitionData = fetchOffsetForTimestamp(fullPartitionName, times, true);
                        responseData.put(topic, partitionData);
                    });

        });

        CompletableFuture
                .allOf(responseData.values().toArray(new CompletableFuture<?>[0]))
                .whenComplete((ignore, ex) -> {
                    ListOffsetResponse response =
                            new ListOffsetResponse(CoreUtils.mapValue(responseData, CompletableFuture::join));
                    resultFuture.complete(response);
                });
    }

    // get offset from underline managedLedger
    @Override
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

    private Map<TopicPartition, Errors> nonExistingTopicErrors() {
        // TODO: The check for the existence of the topic is missing
        return Maps.newHashMap();
    }

    @VisibleForTesting
    Map<TopicPartition, OffsetAndMetadata> convertOffsetCommitRequestRetentionMs(
            Map<TopicPartition, OffsetCommitRequest.PartitionData> convertedOffsetData,
            long retentionTime,
            short apiVersion,
            long currentTimeStamp,
            long configOffsetsRetentionMs) {

        // commit from kafka
        // > for version 1 and beyond store offsets in offset manager
        // > compute the retention time based on the request version:
        // commit from kafka

        long offsetRetention;
        if (apiVersion <= 1 || retentionTime == OffsetCommitRequest.DEFAULT_RETENTION_TIME) {
            offsetRetention = configOffsetsRetentionMs;
        } else {
            offsetRetention = retentionTime;
        }

        // commit from kafka
        // > commit timestamp is always set to now.
        // > "default" expiration timestamp is now + retention (and retention may be overridden if v2)

        // > expire timestamp is computed differently for v1 and v2.
        // >  - If v1 and no explicit commit timestamp is provided we use default expiration timestamp.
        // >  - If v1 and explicit commit timestamp is provided we calculate retention from
        // >    that explicit commit timestamp
        // >  - If v2 we use the default expiration timestamp
        // commit from kafka

        long defaultExpireTimestamp = offsetRetention + currentTimeStamp;


        long finalOffsetRetention = offsetRetention;
        return CoreUtils.mapValue(convertedOffsetData, (partitionData) -> {

            String metadata;
            if (partitionData.metadata == null) {
                metadata = OffsetMetadata.NO_METADATA;
            } else {
                metadata = partitionData.metadata;
            }

            long expireTimeStamp;
            if (partitionData.timestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP) {
                expireTimeStamp = defaultExpireTimestamp;
            } else {
                expireTimeStamp = finalOffsetRetention + partitionData.timestamp;
            }

            return OffsetAndMetadata.apply(
                    partitionData.offset,
                    metadata,
                    currentTimeStamp,
                    expireTimeStamp);
        });

    }

    @Override
    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(offsetCommit.getRequest() instanceof OffsetCommitRequest);
        checkState(getGroupCoordinator() != null,
                "Group Coordinator not started");

        OffsetCommitRequest request = (OffsetCommitRequest) offsetCommit.getRequest();

        // TODO not process nonExistingTopic at this time.
        Map<TopicPartition, Errors> nonExistingTopicErrors = nonExistingTopicErrors(request);
        Map<TopicPartition, Errors> unauthorizedTopicErrors = Maps.newConcurrentMap();

        if (request.offsetData().isEmpty()) {
            resultFuture.complete(new OffsetCommitResponse(Maps.newHashMap()));
            return;
        }
        // convert raw topic name to KoP full name
        // we need to ensure that topic name in __consumer_offsets is globally unique
        Map<TopicPartition, OffsetCommitRequest.PartitionData> convertedOffsetData = Maps.newConcurrentMap();
        Map<TopicPartition, TopicPartition> replacingIndex = Maps.newConcurrentMap();
        AtomicInteger unfinishedAuthorizationCount = new AtomicInteger(request.offsetData().size());

        Consumer<Runnable> completeOne = (action) -> {
            // When complete one authorization or failed, will do the action first.
            action.run();
            if (unfinishedAuthorizationCount.decrementAndGet() == 0) {
                if (log.isTraceEnabled()) {
                    StringBuffer traceInfo = new StringBuffer();
                    replacingIndex.forEach((inner, outer) ->
                            traceInfo.append(String.format("\tinnerName:%s, outerName:%s%n", inner, outer)));
                    log.trace("OFFSET_COMMIT TopicPartition relations: \n{}", traceInfo);
                }
                if (convertedOffsetData.isEmpty()) {
                    Map<TopicPartition, Errors> offsetCommitResult = Maps.newHashMap();

                    offsetCommitResult.putAll(nonExistingTopicErrors);
                    offsetCommitResult.putAll(unauthorizedTopicErrors);

                    OffsetCommitResponse response = new OffsetCommitResponse(offsetCommitResult);
                    resultFuture.complete(response);

                } else {
                    Map<TopicPartition, OffsetAndMetadata> convertedPartitionData =
                            convertOffsetCommitRequestRetentionMs(
                                    convertedOffsetData,
                                    request.retentionTime(),
                                    offsetCommit.getHeader().apiVersion(),
                                    Time.SYSTEM.milliseconds(),
                                    getGroupCoordinator().offsetConfig().offsetsRetentionMs()
                            );

                    getGroupCoordinator().handleCommitOffsets(
                            request.groupId(),
                            request.memberId(),
                            request.generationId(),
                            convertedPartitionData
                    ).thenAccept(offsetCommitResult -> {
                        // recover to original topic name
                        replaceTopicPartition(offsetCommitResult, replacingIndex);

                        offsetCommitResult.putAll(nonExistingTopicErrors);
                        offsetCommitResult.putAll(unauthorizedTopicErrors);

                        OffsetCommitResponse response = new OffsetCommitResponse(offsetCommitResult);
                        resultFuture.complete(response);
                    });

                }
            }
        };
        request.offsetData().forEach((tp, partitionData) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(tp.topic());
            } catch (KoPTopicException e) {
                log.warn("Invalid topic name: {}", tp.topic(), e);
                completeOne.accept(() -> nonExistingTopicErrors.put(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION));
                return;
            }
            String fullTopicName = kopTopic.getFullName();
            authorize(AclOperation.READ, Resource.of(ResourceType.TOPIC, fullTopicName))
                    .whenComplete((isAuthorized, ex) -> {
                        if (ex != null) {
                            log.error("OffsetCommit authorize failed, topic - {}. {}",
                                    fullTopicName, ex.getMessage());
                            completeOne.accept(
                                    () -> unauthorizedTopicErrors.put(tp, Errors.TOPIC_AUTHORIZATION_FAILED));
                            return;
                        }
                        if (!isAuthorized) {
                            completeOne.accept(
                                    () -> unauthorizedTopicErrors.put(tp, Errors.TOPIC_AUTHORIZATION_FAILED));
                            return;
                        }
                        completeOne.accept(() -> {
                            TopicPartition newTopicPartition = new TopicPartition(
                                    new KopTopic(tp.topic()).getFullName(), tp.partition());

                            convertedOffsetData.put(newTopicPartition, partitionData);
                            replacingIndex.put(newTopicPartition, tp);
                        });
                    });
        });
    }

    @Override
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

        MessageFetchContext.get(this, fetch, resultFuture, fetchPurgatory).handleFetch();
    }

    @Override
    protected void handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(joinGroup.getRequest() instanceof JoinGroupRequest);
        checkState(getGroupCoordinator() != null,
            "Group Coordinator not started");

        JoinGroupRequest request = (JoinGroupRequest) joinGroup.getRequest();

        Map<String, byte[]> protocols = new HashMap<>();
        request.groupProtocols()
            .stream()
            .forEach(protocol -> protocols.put(protocol.name(), Utils.toArray(protocol.metadata())));
        getGroupCoordinator().handleJoinGroup(
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

    @Override
    protected void handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(syncGroup.getRequest() instanceof SyncGroupRequest);
        SyncGroupRequest request = (SyncGroupRequest) syncGroup.getRequest();

        groupIds.add(request.groupId());
        getGroupCoordinator().handleSyncGroup(
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

    @Override
    protected void handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(heartbeat.getRequest() instanceof HeartbeatRequest);
        HeartbeatRequest request = (HeartbeatRequest) heartbeat.getRequest();

        // let the coordinator to handle heartbeat
        getGroupCoordinator().handleHeartbeat(
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
        getGroupCoordinator().handleLeaveGroup(
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
                KeyValue<Errors, GroupSummary> describeResult = getGroupCoordinator()
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
        KeyValue<Errors, List<GroupOverview>> listResult = getGroupCoordinator().handleListGroups();
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

        Map<String, Errors> deleteResult = getGroupCoordinator().handleDeleteGroups(request.groups());
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

        final Map<String, ApiError> result = Maps.newConcurrentMap();
        final Map<String, TopicDetails> validTopics = Maps.newHashMap();
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
            return;
        }

        final AtomicInteger validTopicsCount = new AtomicInteger(validTopics.size());
        final Map<String, TopicDetails> authorizedTopics = Maps.newConcurrentMap();
        Runnable createTopicsAsync = () -> {
            if (authorizedTopics.isEmpty()) {
                resultFuture.complete(new CreateTopicsResponse(result));
                return;
            }
            // TODO: handle request.validateOnly()
            adminManager.createTopicsAsync(authorizedTopics, request.timeout()).thenApply(validResult -> {
                result.putAll(validResult);
                resultFuture.complete(new CreateTopicsResponse(result));
                return null;
            });
        };

        BiConsumer<String, TopicDetails> completeOneTopic = (topic, topicDetails) -> {
            authorizedTopics.put(topic, topicDetails);
            if (validTopicsCount.decrementAndGet() == 0) {
                createTopicsAsync.run();
            }
        };
        BiConsumer<String, ApiError> completeOneErrorTopic = (topic, error) -> {
            result.put(topic, error);
            if (validTopicsCount.decrementAndGet() == 0) {
                createTopicsAsync.run();
            }
        };
        validTopics.forEach((topic, details) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(topic);
            } catch (KoPTopicException e) {
                completeOneErrorTopic.accept(topic, ApiError.fromThrowable(e));
                return;
            }
            String fullTopicName = kopTopic.getFullName();
            authorize(AclOperation.CREATE, Resource.of(ResourceType.TOPIC, fullTopicName))
                    .whenComplete((isAuthorized, ex) -> {
                       if (ex != null) {
                           log.error("CreateTopics authorize failed, topic - {}. {}",
                                   fullTopicName, ex.getMessage());
                           completeOneErrorTopic
                                   .accept(topic, new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, ex.getMessage()));
                           return;
                       }
                       if (!isAuthorized) {
                           completeOneErrorTopic
                                   .accept(topic, new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, null));
                           return;
                       }
                       completeOneTopic.accept(topic, details);
                    });
        });


    }

    @Override
    protected void handleDescribeConfigs(KafkaHeaderAndRequest describeConfigs,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(describeConfigs.getRequest() instanceof DescribeConfigsRequest);
        DescribeConfigsRequest request = (DescribeConfigsRequest) describeConfigs.getRequest();

        if (request.resources().isEmpty()) {
            resultFuture.complete(new DescribeConfigsResponse(0, Maps.newHashMap()));
            return;
        }

        Collection<ConfigResource> authorizedResources = Collections.synchronizedList(new ArrayList<>());
        Map<ConfigResource, DescribeConfigsResponse.Config> failedConfigResourceMap =
                Maps.newConcurrentMap();
        AtomicInteger unfinishedAuthorizationCount = new AtomicInteger(request.resources().size());


        Consumer<Runnable> completeOne = (action) -> {
            // When complete one authorization or failed, will do the action first.
            action.run();
            if (unfinishedAuthorizationCount.decrementAndGet() == 0) {
                adminManager.describeConfigsAsync(authorizedResources.stream()
                        .collect(Collectors.toMap(
                                resource -> resource,
                                resource -> Optional.ofNullable(request.configNames(resource)).map(HashSet::new)
                        ))
                ).thenApply(configResourceConfigMap -> {
                    configResourceConfigMap.putAll(failedConfigResourceMap);
                    resultFuture.complete(new DescribeConfigsResponse(0, configResourceConfigMap));
                    return null;
                });
            }
        };

        // Do authorization for each of resource
        request.resources().forEach(configResource -> {
            switch (configResource.type()) {
                case TOPIC:
                    KopTopic kopTopic;
                    try {
                        kopTopic = new KopTopic(configResource.name());
                    } catch (KoPTopicException e) {
                        completeOne.accept(() -> {
                            final ApiError error = new ApiError(
                                    Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                    "Topic " + configResource.name() + " doesn't exist");
                            failedConfigResourceMap.put(configResource, new DescribeConfigsResponse.Config(
                                    error, Collections.emptyList()));
                        });
                        return;
                    }
                    String fullTopicName = kopTopic.getFullName();
                    authorize(AclOperation.DESCRIBE_CONFIGS, Resource.of(ResourceType.TOPIC, fullTopicName))
                            .whenComplete((isAuthorized, ex) -> {
                                if (ex != null) {
                                    log.error("DescribeConfigs in topic authorize failed, topic - {}. {}",
                                            fullTopicName, ex.getMessage());
                                    completeOne.accept(() -> failedConfigResourceMap.put(configResource,
                                            new DescribeConfigsResponse.Config(
                                                    new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, null),
                                                    Collections.emptyList())));
                                    return;
                                }
                                if (isAuthorized) {
                                    completeOne.accept(() -> authorizedResources.add(configResource));
                                    return;
                                }
                                completeOne.accept(() -> failedConfigResourceMap.put(configResource,
                                        new DescribeConfigsResponse.Config(
                                                new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, null),
                                                Collections.emptyList())));
                            });
                    break;
                case BROKER:
                    // Current KoP don't support Broker Resource.
                case UNKNOWN:
                default:
                    completeOne.accept(() -> log.error("KoP doesn't support resource type: " + configResource.type()));
                    break;
            }
        });

    }

    @Override
    protected void handleInitProducerId(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                        CompletableFuture<AbstractResponse> response) {
        InitProducerIdRequest request = (InitProducerIdRequest) kafkaHeaderAndRequest.getRequest();
        TransactionCoordinator transactionCoordinator = getTransactionCoordinator();
        transactionCoordinator.handleInitProducerId(
                request.transactionalId(), request.transactionTimeoutMs(), Optional.empty(), response);
    }

    @Override
    protected void handleAddPartitionsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                            CompletableFuture<AbstractResponse> response) {
        AddPartitionsToTxnRequest request = (AddPartitionsToTxnRequest) kafkaHeaderAndRequest.getRequest();
        List<TopicPartition> partitionsToAdd = request.partitions();
        Map<TopicPartition, Errors> unauthorizedTopicErrors = Maps.newConcurrentMap();
        Map<TopicPartition, Errors> nonExistingTopicErrors = Maps.newConcurrentMap();
        Set<TopicPartition> authorizedPartitions = Sets.newConcurrentHashSet();

        AtomicInteger unfinishedAuthorizationCount = new AtomicInteger(partitionsToAdd.size());
        Consumer<Runnable> completeOne = (action) -> {
            action.run();
            if (unfinishedAuthorizationCount.decrementAndGet() == 0) {
                if (!unauthorizedTopicErrors.isEmpty() || !nonExistingTopicErrors.isEmpty()) {
                    Map<TopicPartition, Errors> partitionErrors = Maps.newHashMap();
                    partitionErrors.putAll(unauthorizedTopicErrors);
                    partitionErrors.putAll(nonExistingTopicErrors);
                    for (TopicPartition topicPartition : authorizedPartitions) {
                        partitionErrors.put(topicPartition, Errors.OPERATION_NOT_ATTEMPTED);
                    }
                    response.complete(new AddPartitionsToTxnResponse(0, partitionErrors));
                } else {
                    TransactionCoordinator transactionCoordinator = getTransactionCoordinator();
                    transactionCoordinator.handleAddPartitionsToTransaction(request.transactionalId(),
                            request.producerId(), request.producerEpoch(), partitionsToAdd, response);
                }
            }
        };
        partitionsToAdd.forEach(tp -> {
            String fullPartitionName;
            try {
                fullPartitionName = KopTopic.toString(tp);
            } catch (KoPTopicException e) {
                log.warn("Invalid topic name: {}", tp.topic(), e);
                completeOne.accept(() -> nonExistingTopicErrors.put(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION));
                return;
            }
            authorize(AclOperation.WRITE, Resource.of(ResourceType.TOPIC, fullPartitionName))
                    .whenComplete((isAuthorized, ex) -> {
                if (ex != null) {
                    log.error("AddPartitionsToTxn topic authorize failed, topic - {}. {}",
                            fullPartitionName, ex.getMessage());
                    completeOne.accept(() -> unauthorizedTopicErrors.put(tp, Errors.TOPIC_AUTHORIZATION_FAILED));
                    return;
                }
                if (!isAuthorized) {
                    completeOne.accept(() -> unauthorizedTopicErrors.put(tp, Errors.TOPIC_AUTHORIZATION_FAILED));
                    return;
                }
                completeOne.accept(() -> authorizedPartitions.add(tp));
            });
        });
    }

    @Override
    protected void handleAddOffsetsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> response) {
        AddOffsetsToTxnRequest request = (AddOffsetsToTxnRequest) kafkaHeaderAndRequest.getRequest();
        int partition = getGroupCoordinator().partitionFor(request.consumerGroupId());
        String offsetTopicName = getGroupCoordinator().getGroupManager().getOffsetConfig().offsetsTopicName();
        TransactionCoordinator transactionCoordinator = getTransactionCoordinator();
        transactionCoordinator.handleAddPartitionsToTransaction(
                request.transactionalId(),
                request.producerId(),
                request.producerEpoch(),
                Collections.singletonList(new TopicPartition(offsetTopicName, partition)), response);
    }

    @Override
    protected void handleTxnOffsetCommit(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> response) {
        TxnOffsetCommitRequest request = (TxnOffsetCommitRequest) kafkaHeaderAndRequest.getRequest();

        if (request.offsets().isEmpty()) {
            response.complete(new TxnOffsetCommitResponse(0, Maps.newHashMap()));
            return;
        }
        // TODO not process nonExistingTopic at this time.
        Map<TopicPartition, Errors> nonExistingTopicErrors = nonExistingTopicErrors();
        Map<TopicPartition, Errors> unauthorizedTopicErrors = Maps.newConcurrentMap();

        // convert raw topic name to KoP full name
        // we need to ensure that topic name in __consumer_offsets is globally unique
        Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> convertedOffsetData = Maps.newConcurrentMap();
        Map<TopicPartition, TopicPartition> replacingIndex = Maps.newHashMap();

        AtomicInteger unfinishedAuthorizationCount = new AtomicInteger(request.offsets().size());

        Consumer<Runnable> completeOne = (action) -> {
            action.run();
            if (unfinishedAuthorizationCount.decrementAndGet() == 0) {
                if (log.isTraceEnabled()) {
                    StringBuffer traceInfo = new StringBuffer();
                    replacingIndex.forEach((inner, outer) ->
                            traceInfo.append(String.format("\tinnerName:%s, outerName:%s%n", inner, outer)));
                    log.trace("TXN_OFFSET_COMMIT TopicPartition relations: \n{}", traceInfo.toString());
                }

                getGroupCoordinator().handleTxnCommitOffsets(
                        request.consumerGroupId(),
                        request.producerId(),
                        request.producerEpoch(),
                        convertTxnOffsets(convertedOffsetData)).whenComplete((resultMap, throwable) -> {

                    // recover to original topic name
                    replaceTopicPartition(resultMap, replacingIndex);

                    resultMap.putAll(nonExistingTopicErrors);
                    resultMap.putAll(unauthorizedTopicErrors);
                    response.complete(new TxnOffsetCommitResponse(0, resultMap));
                });
            }
        };
        request.offsets().forEach((tp, commitOffset) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(tp.topic());
            } catch (KoPTopicException e) {
                log.warn("Invalid topic name: {}", tp.topic(), e);
                completeOne.accept(() -> nonExistingTopicErrors.put(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION));
                return;
            }
            String fullTopicName = kopTopic.getFullName();

            authorize(AclOperation.READ, Resource.of(ResourceType.TOPIC, fullTopicName))
                    .whenComplete((isAuthorized, ex) -> {
                        if (ex != null) {
                            log.error("TxnOffsetCommit authorize failed, topic - {}. {}",
                                    fullTopicName, ex.getMessage());
                            completeOne.accept(
                                    () -> unauthorizedTopicErrors.put(tp, Errors.TOPIC_AUTHORIZATION_FAILED));
                            return;
                        }
                        if (!isAuthorized) {
                            completeOne.accept(()-> unauthorizedTopicErrors.put(tp, Errors.TOPIC_AUTHORIZATION_FAILED));
                            return;
                        }
                        completeOne.accept(()->{
                            TopicPartition newTopicPartition = new TopicPartition(fullTopicName, tp.partition());
                            convertedOffsetData.put(newTopicPartition, commitOffset);
                            replacingIndex.put(newTopicPartition, tp);
                        });
                    });
        });
    }

    private Map<TopicPartition, OffsetAndMetadata> convertTxnOffsets(
                        Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsetsMap) {
        long currentTimestamp = SystemTime.SYSTEM.milliseconds();
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> entry : offsetsMap.entrySet()) {
            TxnOffsetCommitRequest.CommittedOffset partitionData = entry.getValue();
            String metadata;
            if (partitionData.metadata() == null) {
                metadata = OffsetAndMetadata.NoMetadata;
            } else {
                metadata = partitionData.metadata();
            }
            offsetAndMetadataMap.put(entry.getKey(),
                    OffsetAndMetadata.apply(partitionData.offset(), metadata, currentTimestamp, -1));
        }
        return offsetAndMetadataMap;
    }

    @Override
    protected void handleEndTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                CompletableFuture<AbstractResponse> response) {
        EndTxnRequest request = (EndTxnRequest) kafkaHeaderAndRequest.getRequest();
        TransactionCoordinator transactionCoordinator = getTransactionCoordinator();
        transactionCoordinator.handleEndTransaction(
                request.transactionalId(),
                request.producerId(),
                request.producerEpoch(),
                request.command(),
                response);
    }

    @Override
    protected void handleWriteTxnMarkers(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> response) {
        WriteTxnMarkersRequest request = (WriteTxnMarkersRequest) kafkaHeaderAndRequest.getRequest();
        Map<Long, Map<TopicPartition, Errors>> resultMap = new HashMap<>();
        List<CompletableFuture<Void>> resultFutureList = new ArrayList<>();
        TransactionCoordinator transactionCoordinator = getTransactionCoordinator();
        for (WriteTxnMarkersRequest.TxnMarkerEntry txnMarkerEntry : request.markers()) {
            Map<TopicPartition, Errors> partitionErrorsMap =
                    resultMap.computeIfAbsent(txnMarkerEntry.producerId(), pid -> new HashMap<>());

            for (TopicPartition topicPartition : txnMarkerEntry.partitions()) {
                CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                CompletableFuture<Long> completableFuture = writeTxnMarker(
                        topicPartition,
                        txnMarkerEntry.transactionResult(),
                        txnMarkerEntry.producerId(),
                        txnMarkerEntry.producerEpoch());
                completableFuture.whenComplete((offset, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to write txn marker for partition {}", topicPartition, throwable);
                        partitionErrorsMap.put(topicPartition, Errors.forException(throwable));
                        return;
                    }
                    if (offset == null) {
                        partitionErrorsMap.put(topicPartition, Errors.LEADER_NOT_AVAILABLE);
                        return;
                    }

                    CompletableFuture<Void> handleGroupFuture;
                    if (TopicName.get(topicPartition.topic()).getLocalName().equals(GROUP_METADATA_TOPIC_NAME)) {
                        handleGroupFuture = getGroupCoordinator().scheduleHandleTxnCompletion(
                                txnMarkerEntry.producerId(),
                                Lists.newArrayList(topicPartition).stream(),
                                txnMarkerEntry.transactionResult());
                    } else {
                        handleGroupFuture = CompletableFuture.completedFuture(null);
                    }

                    handleGroupFuture.whenComplete((ignored, handleGroupThrowable) -> {
                        if (handleGroupThrowable != null) {
                            log.error("Failed to handle group end txn for partition {}",
                                    topicPartition, handleGroupThrowable);
                            partitionErrorsMap.put(topicPartition, Errors.forException(handleGroupThrowable));
                            resultFuture.completeExceptionally(handleGroupThrowable);
                            return;
                        }
                        String fullPartitionName = KopTopic.toString(topicPartition);
                        TopicName topicName = TopicName.get(fullPartitionName);

                        long firstOffset = transactionCoordinator.removeActivePidOffset(
                                topicName, txnMarkerEntry.producerId());
                        long lastStableOffset = transactionCoordinator.getLastStableOffset(topicName, offset);

                        if (txnMarkerEntry.transactionResult().equals(TransactionResult.ABORT)) {
                            transactionCoordinator.addAbortedIndex(AbortedIndexEntry.builder()
                                    .version(request.version())
                                    .pid(txnMarkerEntry.producerId())
                                    .firstOffset(firstOffset)
                                    .lastOffset(offset)
                                    .lastStableOffset(lastStableOffset)
                                    .build());
                        }
                        partitionErrorsMap.put(topicPartition, Errors.NONE);
                        resultFuture.complete(null);
                    });
                });
                resultFutureList.add(resultFuture);
            }
        }
        FutureUtil.waitForAll(resultFutureList).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("Write txn mark fail!", throwable);
                response.complete(new WriteTxnMarkersResponse(resultMap));
                return;
            }
            response.complete(new WriteTxnMarkersResponse(resultMap));
        });
    }

    @Override
    protected void handleDeleteTopics(KafkaHeaderAndRequest deleteTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteTopics.getRequest() instanceof DeleteTopicsRequest);
        DeleteTopicsRequest request = (DeleteTopicsRequest) deleteTopics.getRequest();
        Set<String> topicsToDelete = request.topics();
        if (topicsToDelete == null || topicsToDelete.isEmpty()) {
            resultFuture.complete(new DeleteTopicsResponse(Maps.newHashMap()));
            return;
        }
        Map<String, Errors> deleteTopicsResponse = Maps.newConcurrentMap();
        AtomicInteger topicToDeleteCount = new AtomicInteger(topicsToDelete.size());
        BiConsumer<String, Errors> completeOne = (topic, errors) -> {
            deleteTopicsResponse.put(topic, errors);
            if (errors == Errors.NONE) {
                // create topic ZNode to trigger the coordinator DeleteTopicsEvent event
                ZooKeeperUtils.tryCreatePath(pulsarService.getZkClient(),
                        KopEventManager.getDeleteTopicsPath() + "/"
                                + TopicNameUtils.getTopicNameWithUrlEncoded(topic),
                        new byte[0]);
            }
            if (topicToDeleteCount.decrementAndGet() == 0) {
                resultFuture.complete(new DeleteTopicsResponse(deleteTopicsResponse));
            }
        };
        topicsToDelete.forEach(topic -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(topic);
            } catch (KoPTopicException e) {
                completeOne.accept(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                return;
            }
            String fullTopicName = kopTopic.getFullName();
            authorize(AclOperation.DELETE, Resource.of(ResourceType.TOPIC, fullTopicName))
                    .whenComplete((isAuthorize, ex) -> {
                        if (ex != null) {
                            log.error("DeleteTopics authorize failed, topic - {}. {}",
                                    fullTopicName, ex.getMessage());
                            completeOne.accept(topic, Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }
                        if (!isAuthorize) {
                            completeOne.accept(topic, Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }
                        adminManager.deleteTopic(fullTopicName,
                                __ -> completeOne.accept(topic, Errors.NONE),
                                __ -> completeOne.accept(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION));
                    });
        });
    }

    @Override
    protected void handleCreatePartitions(KafkaHeaderAndRequest createPartitions,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(createPartitions.getRequest() instanceof CreatePartitionsRequest);
        CreatePartitionsRequest request = (CreatePartitionsRequest) createPartitions.getRequest();

        final Map<String, ApiError> result = Maps.newConcurrentMap();
        final Map<String, NewPartitions> validTopics = Maps.newHashMap();
        final Set<String> duplicateTopics = request.duplicates();

        request.newPartitions().forEach((topic, newPartition) -> {
            if (!duplicateTopics.contains(topic)) {
                validTopics.put(topic, newPartition);
            } else {
                final String errorMessage = "Create topics partitions request from client `"
                        + createPartitions.getHeader().clientId()
                        + "` contains multiple entries for the following topics: " + duplicateTopics;
                result.put(topic, new ApiError(Errors.INVALID_REQUEST, errorMessage));
            }
        });

        if (validTopics.isEmpty()) {
            resultFuture.complete(new CreatePartitionsResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, result));
            return;
        }

        final AtomicInteger validTopicsCount = new AtomicInteger(validTopics.size());
        final Map<String, NewPartitions> authorizedTopics = Maps.newConcurrentMap();
        Runnable createPartitionsAsync = () -> {
            if (authorizedTopics.isEmpty()) {
                resultFuture.complete(new CreatePartitionsResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, result));
                return;
            }
            adminManager.createPartitionsAsync(authorizedTopics, request.timeout()).thenApply(validResult -> {
                result.putAll(validResult);
                resultFuture.complete(new CreatePartitionsResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, result));
                return null;
            });
        };

        BiConsumer<String, NewPartitions> completeOneTopic = (topic, newPartitions) -> {
            authorizedTopics.put(topic, newPartitions);
            if (validTopicsCount.decrementAndGet() == 0) {
                createPartitionsAsync.run();
            }
        };
        BiConsumer<String, ApiError> completeOneErrorTopic = (topic, error) -> {
            result.put(topic, error);
            if (validTopicsCount.decrementAndGet() == 0) {
                createPartitionsAsync.run();
            }
        };
        validTopics.forEach((topic, newPartitions) -> {
            try {
                KopTopic kopTopic = new KopTopic(topic);
                String fullTopicName = kopTopic.getFullName();
                authorize(AclOperation.ALTER, Resource.of(ResourceType.TOPIC, fullTopicName))
                        .whenComplete((isAuthorized, ex) -> {
                            if (ex != null) {
                                log.error("CreatePartitions authorize failed, topic - {}. {}",
                                        fullTopicName, ex.getMessage());
                                completeOneErrorTopic.accept(topic,
                                        new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, ex.getMessage()));
                                return;
                            }
                            if (!isAuthorized) {
                                completeOneErrorTopic.accept(topic,
                                        new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, null));
                                return;
                            }
                            completeOneTopic.accept(topic, newPartitions);
                        });
            } catch (KoPTopicException e) {
                completeOneErrorTopic.accept(topic, ApiError.fromThrowable(e));
            }
        });
    }

    /**
     * Write the txn marker to the topic partition.
     *
     * @param topicPartition
     */
    private CompletableFuture<Long> writeTxnMarker(TopicPartition topicPartition,
                                TransactionResult transactionResult,
                                long producerId,
                                short producerEpoch) {
        CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        String fullPartitionName = KopTopic.toString(topicPartition);
        TopicName topicName = TopicName.get(fullPartitionName);
        topicManager.getTopic(topicName.toString())
                .whenComplete((persistentTopicOpt, throwable) -> {
                    if (throwable != null) {
                        offsetFuture.completeExceptionally(throwable);
                        return;
                    }
                    if (!persistentTopicOpt.isPresent()) {
                        offsetFuture.complete(null);
                        return;
                    }
                    PersistentTopic persistentTopic = persistentTopicOpt.get();
                    persistentTopic.publishMessage(generateTxnMarker(transactionResult, producerId, producerEpoch),
                            MessagePublishContext.get(offsetFuture, persistentTopic,
                                    1, SystemTime.SYSTEM.milliseconds()));
                });
        return offsetFuture;
    }

    private ByteBuf generateTxnMarker(TransactionResult transactionResult, long producerId, short producerEpoch) {
        ControlRecordType controlRecordType;
        MarkerType markerType;
        if (transactionResult.equals(TransactionResult.COMMIT)) {
            markerType = MarkerType.TXN_COMMIT;
            controlRecordType = ControlRecordType.COMMIT;
        } else {
            markerType = MarkerType.TXN_ABORT;
            controlRecordType = ControlRecordType.ABORT;
        }
        EndTransactionMarker marker = new EndTransactionMarker(controlRecordType, 0);
        MemoryRecords memoryRecords = MemoryRecords.withEndTransactionMarker(producerId, producerEpoch, marker);

        ByteBuf byteBuf = Unpooled.wrappedBuffer(memoryRecords.buffer());
        MessageMetadata messageMetadata = new MessageMetadata()
                .setTxnidMostBits(producerId)
                .setTxnidLeastBits(producerEpoch)
                .setMarkerType(markerType.getValue())
                .setPublishTime(SystemTime.SYSTEM.milliseconds())
                .setProducerName("")
                .setSequenceId(0L);
        return Commands.serializeMetadataAndPayload(Commands.ChecksumType.None, messageMetadata, byteBuf);
    }

    private SaslHandshakeResponse checkSaslMechanism(String mechanism) {
        if (getKafkaConfig().getSaslAllowedMechanisms().contains(mechanism)) {
            return new SaslHandshakeResponse(Errors.NONE, getKafkaConfig().getSaslAllowedMechanisms());
        }
        return new SaslHandshakeResponse(Errors.UNSUPPORTED_SASL_MECHANISM, new HashSet<>());
    }

    @Override
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
            KafkaTopicManager.removeTopicManagerCache(topic.toString());

            returnFuture.complete(Optional.empty());
            return returnFuture;
        }

        if (log.isDebugEnabled()) {
            log.debug("Found broker for topic {} puslarAddress: {}",
                topic, pulsarAddress);
        }

        // get kop address from cache to prevent query zk each time.
        final CompletableFuture<Optional<String>> future = KafkaTopicManager.KOP_ADDRESS_CACHE.get(topic.toString());
        if (future != null) {
            return future;
        }

        if (advertisedEndPoint.isMultiListener()) {
            // if kafkaProtocolMap is set, the lookup result is the advertised address
            String kafkaAdvertisedAddress = String.format("%s://%s:%s", advertisedEndPoint.getSecurityProtocol().name,
                    pulsarAddress.getHostName(), pulsarAddress.getPort());
            KafkaTopicManager.KOP_ADDRESS_CACHE.put(topic.toString(), returnFuture);
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
                    KafkaTopicManager.removeTopicManagerCache(topic.toString());
                    return;
                }

                // Get a list of ServiceLookupData for each matchBroker.
                List<CompletableFuture<Optional<LocalBrokerData>>> list = matchBrokers.stream()
                    .map(matchBroker -> localBrokerDataCache.get(
                            String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, matchBroker)))
                    .collect(Collectors.toList());

                FutureUtil.waitForAll(list)
                    .whenComplete((ignore, th) -> {
                            if (th != null) {
                                log.error("Error in getDataAsync() for {}", pulsarAddress, th);
                                returnFuture.complete(Optional.empty());
                                KafkaTopicManager.removeTopicManagerCache(topic.toString());
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
                                        KafkaTopicManager.KOP_ADDRESS_CACHE.put(topic.toString(), returnFuture);
                                        returnFuture.complete(data.getProtocol(KafkaProtocolHandler.PROTOCOL_NAME));
                                        return;
                                    }
                                }
                            } catch (Exception e) {
                                log.error("Error in {} lookupFuture get: ", pulsarAddress, e);
                                returnFuture.complete(Optional.empty());
                                KafkaTopicManager.removeTopicManagerCache(topic.toString());
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

    public CompletableFuture<PartitionMetadata> findBroker(TopicName topic) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Handle Lookup for {}", ctx.channel(), topic);
        }
        CompletableFuture<PartitionMetadata> returnFuture = new CompletableFuture<>();

        topicManager.getTopicBroker(topic.toString(),
                advertisedEndPoint.isMultiListener() ? advertisedEndPoint.getListenerName() : null)
                .thenApply(address -> getProtocolDataToAdvertise(address, topic))
                .thenAccept(kopAddressFuture -> kopAddressFuture.thenAccept(listenersOptional -> {
                    if (!listenersOptional.isPresent()) {
                        log.error("Not get advertise data for Kafka topic:{}.", topic);
                        KafkaTopicManager.removeTopicManagerCache(topic.toString());
                        returnFuture.complete(null);
                        return;
                    }

                    // It's the `kafkaAdvertisedListeners` config that's written to ZK
                    final String listeners = listenersOptional.get();
                    final EndPoint endPoint =
                            (tlsEnabled ? EndPoint.getSslEndPoint(listeners) :
                                    EndPoint.getPlainTextEndPoint(listeners));
                    final Node node = newNode(endPoint.getInetAddress());

                    if (log.isDebugEnabled()) {
                        log.debug("Found broker localListeners: {} for topicName: {}, "
                                        + "localListeners: {}, found Listeners: {}",
                                listeners, topic, advertisedListeners, listeners);
                    }

                    // here we found topic broker: broker2, but this is in broker1,
                    // how to clean the lookup cache?
                    if (!advertisedListeners.contains(endPoint.getOriginalListener())) {
                        KafkaTopicManager.removeTopicManagerCache(topic.toString());
                    }
                    returnFuture.complete(newPartitionMetadata(topic, node));
                })).exceptionally(throwable -> {
                    log.error("Not get advertise data for Kafka topic:{}. throwable: [{}]",
                            topic, throwable.getMessage());
                    KafkaTopicManager.removeTopicManagerCache(topic.toString());
                    returnFuture.complete(null);
                    return null;
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
        return newNode(advertisedEndPoint.getInetAddress());
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

    private static MemoryRecords validateRecords(short version, TopicPartition topicPartition, MemoryRecords records) {
        if (version >= 3) {
            Iterator<MutableRecordBatch> iterator = records.batches().iterator();
            if (!iterator.hasNext()) {
                throw new InvalidRecordException("Produce requests with version " + version + " must have at least "
                    + "one record batch");
            }

            MutableRecordBatch entry = iterator.next();
            if (entry.magic() != RecordBatch.MAGIC_VALUE_V2) {
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to "
                    + "contain record batches with magic version 2");
            }

            if (iterator.hasNext()) {
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to "
                    + "contain exactly one record batch");
            }
        }

        int validBytesCount = 0;
        for (RecordBatch batch : records.batches()) {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && batch.baseOffset() != 0) {
                throw new InvalidRecordException("The baseOffset of the record batch in the append to "
                    + topicPartition + " should be 0, but it is " + batch.baseOffset());
            }

            batch.ensureValid();
            validBytesCount += batch.sizeInBytes();
        }

        if (validBytesCount < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length "
                + validBytesCount + " to log for " + topicPartition
                + ". A possible cause is corrupted produce request.");
        }

        MemoryRecords validRecords;
        if (validBytesCount == records.sizeInBytes()) {
            validRecords = records;
        } else {
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytesCount);
            validRecords = MemoryRecords.readableRecords(validByteBuffer);
        }

        return validRecords;
    }

    private void updateProducerStats(final TopicPartition topicPartition, final int numMessages, final int numBytes) {
        requestStats.getStatsLogger()
                .scopeLabel(TOPIC_SCOPE, topicPartition.topic())
                .scopeLabel(PARTITION_SCOPE, String.valueOf((topicPartition.partition())))
                .getCounter(BYTES_IN)
                .add(numBytes);

        requestStats.getStatsLogger()
                .scopeLabel(TOPIC_SCOPE, topicPartition.topic())
                .scopeLabel(PARTITION_SCOPE, String.valueOf((topicPartition.partition())))
                .getCounter(MESSAGE_IN)
                .add(numMessages);

        RequestStats.BATCH_COUNT_PER_MEMORY_RECORDS_INSTANCE.set(numMessages);
    }

    @VisibleForTesting
    protected CompletableFuture<Boolean> authorize(AclOperation operation, Resource resource) {
        Session session = authenticator != null ? authenticator.session() : null;
        return authorize(operation, resource, session);
    }

    protected CompletableFuture<Boolean> authorize(AclOperation operation, Resource resource, Session session) {
        if (authorizer == null) {
            return CompletableFuture.completedFuture(true);
        }
        if (session == null) {
            return CompletableFuture.completedFuture(false);
        }
        CompletableFuture<Boolean> isAuthorizedFuture = null;
        switch (operation) {
            case READ:
                isAuthorizedFuture = authorizer.canConsumeAsync(session.getPrincipal(), resource);
                break;
            case IDEMPOTENT_WRITE:
            case WRITE:
                isAuthorizedFuture = authorizer.canProduceAsync(session.getPrincipal(), resource);
                break;
            case DESCRIBE:
                isAuthorizedFuture = authorizer.canLookupAsync(session.getPrincipal(), resource);
                break;
            case CREATE:
            case DELETE:
            case ALTER:
            case DESCRIBE_CONFIGS:
            case ALTER_CONFIGS:
                isAuthorizedFuture = authorizer.canManageTenantAsync(session.getPrincipal(), resource);
                break;
            case ANY:
                if (resource.getResourceType() == ResourceType.TENANT) {
                    isAuthorizedFuture = authorizer.canAccessTenantAsync(session.getPrincipal(), resource);
                }
                break;
            case CLUSTER_ACTION:
            case UNKNOWN:
            case ALL:
            default:
                break;
        }
        if (isAuthorizedFuture == null) {
            return FutureUtil.failedFuture(
                    new IllegalStateException("AclOperation [" + operation.name() + "] is not supported."));
        }
        return isAuthorizedFuture;
    }

    /**
     * If we are using kafkaEnableMultiTenantMetadata we need to ensure
     * that the TenantSpec refer to an existing tenant.
     * @param session
     * @return whether the tenant is accessible
     */
    private boolean validateTenantAccessForSession(Session session)
            throws AuthenticationException {
        if (!kafkaConfig.isKafkaEnableMultiTenantMetadata()) {
            // we are not leveraging kafkaEnableMultiTenantMetadata feature
            // the client will access only system tenant
            return true;
        }
        String tenantSpec = session.getPrincipal().getTenantSpec();
        if (tenantSpec == null) {
            // we are not leveraging kafkaEnableMultiTenantMetadata feature
            // the client will access only system tenant
            return true;
        }
        String currentTenant = extractTenantFromTenantSpec(tenantSpec);
        try {
            Boolean granted = authorize(AclOperation.ANY,
                    Resource.of(ResourceType.TENANT, currentTenant), session)
                    .get();
            return granted != null && granted;
        } catch (ExecutionException | InterruptedException err) {
            log.error("Internal error while verifying tenant access", err);
            throw new AuthenticationException("Internal error while verifying tenant access:" + err, err);
        }
    }
}
