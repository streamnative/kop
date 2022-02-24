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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupOverview;
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
import io.streamnative.pulsar.handlers.kop.storage.AppendRecordsContext;
import io.streamnative.pulsar.handlers.kop.storage.PartitionLog;
import io.streamnative.pulsar.handlers.kop.storage.ReplicaManager;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.GroupIdUtils;
import io.streamnative.pulsar.handlers.kop.utils.KafkaRequestUtils;
import io.streamnative.pulsar.handlers.kop.utils.KafkaResponseUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.ListPair;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import io.streamnative.pulsar.handlers.kop.utils.OffsetFinder;
import io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
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
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
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
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * This class contains all the request handling methods.
 */
@Slf4j
@Getter
public class KafkaRequestHandler extends KafkaCommandDecoder {
    private static final String POLICY_ROOT = "/admin/policies/";

    private final PulsarService pulsarService;
    private final KafkaTopicManager topicManager;
    private final TenantContextManager tenantContextManager;
    private final KopBrokerLookupManager kopBrokerLookupManager;

    private final String clusterName;
    private final ScheduledExecutorService executor;
    private final PulsarAdmin admin;
    private final MetadataStoreExtended metadataStore;
    private final SaslAuthenticator authenticator;
    private final Authorizer authorizer;
    private final AdminManager adminManager;

    private final Boolean tlsEnabled;
    private final EndPoint advertisedEndPoint;
    private final boolean skipMessagesWithoutIndex;
    private final int defaultNumPartitions;
    public final int maxReadEntriesNum;
    private final int failedAuthenticationDelayMs;
    // store the group name for current connected client.
    private final ConcurrentHashMap<String, CompletableFuture<String>> currentConnectedGroup;
    private final String groupIdStoredPath;

    @Getter
    private final EntryFormatter entryFormatter;

    private final Set<String> groupIds = new HashSet<>();
    // key is the topic(partition), value is the future that indicates whether the PersistentTopic instance of the key
    // is found.
    private final Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap = new ConcurrentHashMap<>();
    // DelayedOperation for produce and fetch
    private final DelayedOperationPurgatory<DelayedOperation> producePurgatory;
    private final DelayedOperationPurgatory<DelayedOperation> fetchPurgatory;

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

    public String currentNamespacePrefix() {
        String currentTenant = getCurrentTenant(kafkaConfig.getKafkaTenant());
        return MetadataUtils.constructUserTopicsNamespace(currentTenant, kafkaConfig);
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
        throwIfTransactionCoordinatorDisabled();
        return tenantContextManager.getTransactionCoordinator(getCurrentTenant());
    }

    public ReplicaManager getReplicaManager() {
        return tenantContextManager.getReplicaManager(getCurrentTenant());
    }

    public KafkaRequestHandler(PulsarService pulsarService,
                               KafkaServiceConfiguration kafkaConfig,
                               TenantContextManager tenantContextManager,
                               KopBrokerLookupManager kopBrokerLookupManager,
                               AdminManager adminManager,
                               DelayedOperationPurgatory<DelayedOperation> producePurgatory,
                               DelayedOperationPurgatory<DelayedOperation> fetchPurgatory,
                               Boolean tlsEnabled,
                               EndPoint advertisedEndPoint,
                               boolean skipMessagesWithoutIndex,
                               RequestStats requestStats,
                               OrderedScheduler sendResponseScheduler) throws Exception {
        super(requestStats, kafkaConfig, sendResponseScheduler);
        this.pulsarService = pulsarService;
        this.tenantContextManager = tenantContextManager;
        this.kopBrokerLookupManager = kopBrokerLookupManager;
        this.clusterName = kafkaConfig.getClusterName();
        this.executor = pulsarService.getExecutor();
        this.admin = pulsarService.getAdminClient();
        this.metadataStore = pulsarService.getLocalMetadataStore();
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
        this.producePurgatory = producePurgatory;
        this.fetchPurgatory = fetchPurgatory;
        this.tlsEnabled = tlsEnabled;
        this.advertisedEndPoint = advertisedEndPoint;
        this.skipMessagesWithoutIndex = skipMessagesWithoutIndex;
        this.topicManager = new KafkaTopicManager(this);
        this.defaultNumPartitions = kafkaConfig.getDefaultNumPartitions();
        this.maxReadEntriesNum = kafkaConfig.getMaxReadEntriesNum();
        this.entryFormatter = EntryFormatterFactory.create(kafkaConfig);
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
                                  BiConsumer<ApiKeys, Long> registerRequestLatency)
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
        if (unsupportedApiVersion){
            return KafkaResponseUtils.newApiVersions(Errors.UNSUPPORTED_VERSION);
        } else {
            List<ApiVersion> versionList = new ArrayList<>();
            for (ApiKeys apiKey : ApiKeys.values()) {
                if (apiKey.minRequiredInterBrokerMagic <= RecordBatch.CURRENT_MAGIC_VALUE) {
                    switch (apiKey) {
                        case LIST_OFFSETS:
                            // V0 is needed for librdkafka
                            versionList.add(new ApiVersion((short) 2, (short) 0, apiKey.latestVersion()));
                            break;
                        default:
                            versionList.add(new ApiVersion(apiKey));
                    }
                }
            }
            return KafkaResponseUtils.newApiVersions(versionList);
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
    // NOTE: the returned future never completes exceptionally
    private CompletableFuture<Integer> getPartitionedTopicMetadataAsync(String topicName,
                                                                        boolean allowAutoTopicCreation) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        admin.topics().getPartitionedTopicMetadataAsync(topicName).whenComplete((metadata, e) -> {
            if (e == null) {
                if (metadata.partitions > 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("Topic {} has {} partitions", topicName, metadata.partitions);
                    }
                    future.complete(metadata.partitions);
                } else {
                    future.complete(TopicAndMetadata.NON_PARTITIONED_NUMBER);
                }
            } else if (e instanceof PulsarAdminException.NotFoundException) {
                if (allowAutoTopicCreation) {
                    log.info("[{}] Topic {} doesn't exist, auto create it with {} partitions",
                            ctx.channel(), topicName, defaultNumPartitions);
                    admin.topics().createPartitionedTopicAsync(topicName, defaultNumPartitions)
                            .whenComplete((__, createException) -> {
                                if (createException == null) {
                                    future.complete(defaultNumPartitions);
                                } else {
                                    future.complete(TopicAndMetadata.INVALID_PARTITIONS);
                                }
                            });
                } else {
                    log.error("[{}] Topic {} doesn't exist and it's not allowed to auto create partitioned topic",
                            ctx.channel(), topicName, e);
                    future.complete(TopicAndMetadata.INVALID_PARTITIONS);
                }
            } else {
                log.error("[{}] Failed to get partitioned topic {}", ctx.channel(), topicName, e);
                future.complete(TopicAndMetadata.INVALID_PARTITIONS);
            }
        });
        return future;
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
                        .listNamespacesAsync(tenant)
                        .thenAccept(namespaces -> namespaces.forEach(ns -> result.add(tenant + "/" + ns))));
            }
        }
        return CompletableFuture
                .allOf(results.toArray(new CompletableFuture<?>[0]))
                .thenApply(f -> result);
    }

    private List<TopicAndMetadata> analyzeFullTopicNames(final Stream<String> fullTopicNames) {
        // key is the topic name, value is a list of the topic's partition indexes
        final Map<String, List<Integer>> topicToPartitionIndexes = new HashMap<>();
        fullTopicNames.forEach(fullTopicName -> {
            final TopicName topicName = TopicName.get(fullTopicName);
            topicToPartitionIndexes.computeIfAbsent(
                    topicName.getPartitionedTopicName(),
                    ignored -> new ArrayList<>()
            ).add(topicName.getPartitionIndex());
        });
        if (topicToPartitionIndexes.isEmpty()) {
            return Collections.emptyList();
        }

        // Check missed partitions
        final List<TopicAndMetadata> topicAndMetadataList = new ArrayList<>();
        topicToPartitionIndexes.forEach((topic, partitionIndexes) -> {
            Collections.sort(partitionIndexes);
            final int lastIndex = partitionIndexes.get(partitionIndexes.size() - 1);
            if (lastIndex < 0) {
                topicAndMetadataList.add(
                        new TopicAndMetadata(topic, TopicAndMetadata.NON_PARTITIONED_NUMBER));
            } else {
                if (lastIndex == partitionIndexes.size() - 1) {
                    topicAndMetadataList.add(new TopicAndMetadata(topic, partitionIndexes.size()));
                } else {
                    log.warn("The partitions of topic {} is wrong ({}), try to create missed partitions",
                            topic, partitionIndexes.size());
                    admin.topics().createMissedPartitionsAsync(topic);
                }
            }
        });
        return topicAndMetadataList;
    }

    private CompletableFuture<List<String>> authorizeNamespacesAsync(final Collection<String> namespaces,
                                                                     final AclOperation aclOperation) {
        final Map<String, CompletableFuture<Boolean>> futureMap = namespaces.stream().collect(
                Collectors.toMap(
                        namespace -> namespace,
                        namespace -> authorize(aclOperation, Resource.of(ResourceType.NAMESPACE, namespace))
                ));
        return CoreUtils.waitForAll(futureMap.values()).thenApply(__ ->
            futureMap.entrySet().stream().filter(e -> {
                if (!e.getValue().join()) {
                    log.warn("Failed to authorize {} for ACL operation {}", e.getKey(), aclOperation);
                    return false;
                }
                return true;
            }).map(Map.Entry::getKey).collect(Collectors.toList())
        );
    }

    private CompletableFuture<Stream<String>> listAllTopicsFromNamespacesAsync(final List<String> namespaces) {
        return CoreUtils.waitForAll(namespaces.stream()
                .map(namespace -> pulsarService.getNamespaceService()
                        .getListOfPersistentTopics(NamespaceName.get(namespace))
                ).collect(Collectors.toList()),
                topicsStream -> topicsStream.flatMap(List::stream));
    }

    private CompletableFuture<ListPair<String>> authorizeTopicsAsync(final Collection<String> topics,
                                                                     final AclOperation aclOperation) {
        final Map<String, CompletableFuture<Boolean>> futureMap = topics.stream().collect(
                Collectors.toMap(
                        topic -> topic,
                        topic -> authorize(aclOperation, Resource.of(ResourceType.TOPIC, topic))
                ));
        return CoreUtils.waitForAll(futureMap.values()).thenApply(__ ->
                ListPair.of(futureMap.entrySet()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getValue().join()))
                ).map(Map.Entry::getKey));
    }

    private CompletableFuture<List<TopicAndMetadata>> findTopicMetadata(final ListPair<String> listPair,
                                                                        final boolean allowTopicAutoCreation) {
        final Map<String, CompletableFuture<Integer>> futureMap = CoreUtils.listToMap(
                listPair.getSuccessfulList(),
                topic -> getPartitionedTopicMetadataAsync(topic, allowTopicAutoCreation)
        );
        return CoreUtils.waitForAll(futureMap.values()).thenApply(__ ->
                CoreUtils.mapToList(futureMap, (key, value) -> new TopicAndMetadata(key, value.join()))
        ).thenApply(topicAndMetadataList -> {
            // Create a new list in case `topicAndMetadataList` is not modifiable.
            final List<TopicAndMetadata> list = CoreUtils.listToList(listPair.getFailedList(),
                    topic -> new TopicAndMetadata(topic, TopicAndMetadata.AUTHORIZATION_FAILURE));
            list.addAll(topicAndMetadataList);
            return list;
        });
    }

    private CompletableFuture<List<TopicAndMetadata>> getTopicsAsync(MetadataRequest request,
                                                                     Set<String> fullTopicNames) {
        // The implementation of MetadataRequest#isAllTopics() in kafka-clients 2.0 is wrong.
        // Because in version 0, an empty topic list indicates "request metadata for all topics."
        if ((request.topics() == null) || (request.topics().isEmpty() && request.version() == 0)) {
            // clean all cache when get all metadata for librdkafka(<1.0.0).
            KopBrokerLookupManager.clear();
            return expandAllowedNamespaces(kafkaConfig.getKopAllowedNamespaces())
                    .thenCompose(namespaces -> authorizeNamespacesAsync(namespaces, AclOperation.DESCRIBE))
                    .thenCompose(this::listAllTopicsFromNamespacesAsync)
                    .thenApply(this::analyzeFullTopicNames);
        } else {
            final boolean allowTopicCreation = kafkaConfig.isAllowAutoTopicCreation()
                    && request.allowAutoTopicCreation();
            return authorizeTopicsAsync(fullTopicNames, AclOperation.DESCRIBE)
                    .thenCompose(authorizedTopicsPair -> findTopicMetadata(authorizedTopicsPair, allowTopicCreation));
        }
    }

    @Override
    protected void handleTopicMetadataRequest(KafkaHeaderAndRequest metadataHar,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        // Get all kop brokers in local cache
        List<Node> allNodes = Collections.synchronizedList(
                new ArrayList<>(adminManager.getBrokers(advertisedEndPoint.getListenerName())));

        // Each Pulsar broker can manage metadata like controller in Kafka,
        // Kafka's AdminClient needs to find a controller node for metadata management.
        // So here we return an random broker as a controller for the given listenerName.
        final int controllerId = adminManager.getControllerId(advertisedEndPoint.getListenerName());

        final String namespacePrefix = currentNamespacePrefix();
        final MetadataRequest request = (MetadataRequest) metadataHar.getRequest();
        // This map is used to find the original topic name. Both key and value don't have the "-partition-" suffix.
        final Map<String, String> fullTopicNameToOriginal = (request.topics() == null)
                ? Collections.emptyMap()
                : request.topics().stream().collect(
                        Collectors.toMap(
                                topic -> new KopTopic(topic, namespacePrefix).getFullName(),
                                topic -> topic
                        ));
        final Function<String, String> getOriginalTopic = fullTopicName ->
                fullTopicNameToOriginal.getOrDefault(fullTopicName, fullTopicName);

        final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();
        getTopicsAsync(request, fullTopicNameToOriginal.keySet()).whenComplete((topicAndMetadataList, e) -> {
            if (e != null) {
                log.error("[{}] Request {}: Exception fetching metadata, will return null Response",
                        ctx.channel(), metadataHar.getHeader(), e);
                resultFuture.complete(KafkaResponseUtils.newMetadata(
                        allNodes,
                        clusterName,
                        controllerId,
                        Collections.emptyList()));
                return;
            }

            final ListPair<TopicAndMetadata> listPair =
                    ListPair.split(topicAndMetadataList.stream(), TopicAndMetadata::hasNoError);
            CoreUtils.waitForAll(listPair.getSuccessfulList().stream()
                    .map(topicAndMetadata ->
                            topicAndMetadata.lookupAsync(this::findBroker, getOriginalTopic, metadataNamespace)
                    ).collect(Collectors.toList()), topicMetadataStream -> {
                final List<TopicMetadata> topicMetadataList = listPair.getFailedList().stream()
                        .map(metadata -> metadata.toTopicMetadata(getOriginalTopic, metadataNamespace))
                        .collect(Collectors.toList());
                topicMetadataStream.forEach(topicMetadataList::add);
                resultFuture.complete(
                        KafkaResponseUtils.newMetadata(allNodes, clusterName, controllerId, topicMetadataList));
                return null;
            }).exceptionally(lookupException -> {
                log.error("[{}] Unexpected exception during lookup", ctx.channel(), lookupException);
                resultFuture.completeExceptionally(lookupException);
                return null;
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

    @Override
    protected void handleProduceRequest(KafkaHeaderAndRequest produceHar,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(produceHar.getRequest() instanceof ProduceRequest);
        ProduceRequest produceRequest = (ProduceRequest) produceHar.getRequest();

        final int numPartitions = produceRequest.partitionRecordsOrFail().size();
        if (numPartitions == 0) {
            resultFuture.complete(new ProduceResponse(Collections.emptyMap()));
            return;
        }
        final Map<TopicPartition, PartitionResponse> unauthorizedTopicResponsesMap = new ConcurrentHashMap<>();
        final Map<TopicPartition, PartitionResponse> invalidRequestResponses = new HashMap<>();
        final Map<TopicPartition, MemoryRecords> authorizedRequestInfo = new ConcurrentHashMap<>();
        int timeoutMs = produceRequest.timeout();
        String namespacePrefix = currentNamespacePrefix();
        final AtomicInteger unfinishedAuthorizationCount = new AtomicInteger(numPartitions);
        Runnable completeOne = () -> {
            // When complete one authorization or failed, will do the action first.
            if (unfinishedAuthorizationCount.decrementAndGet() == 0) {
                if (authorizedRequestInfo.isEmpty()) {
                    resultFuture.complete(new ProduceResponse(unauthorizedTopicResponsesMap));
                    return;
                }
                AppendRecordsContext appendRecordsContext = AppendRecordsContext.get(
                        topicManager,
                        requestStats,
                        this::startSendOperationForThrottling,
                        this::completeSendOperationForThrottling,
                        pendingTopicFuturesMap);
                getReplicaManager().appendRecords(
                        timeoutMs,
                        false,
                        namespacePrefix,
                        authorizedRequestInfo,
                        PartitionLog.AppendOrigin.Client,
                        appendRecordsContext
                ).whenComplete((response, ex) -> {
                    appendRecordsContext.recycle();
                    if (ex != null) {
                        resultFuture.completeExceptionally(ex.getCause());
                        return;
                    }
                    Map<TopicPartition, PartitionResponse> mergedResponse = new HashMap<>();
                    mergedResponse.putAll(response);
                    mergedResponse.putAll(unauthorizedTopicResponsesMap);
                    mergedResponse.putAll(invalidRequestResponses);
                    resultFuture.complete(new ProduceResponse(mergedResponse));
                });
            }
        };

        produceRequest.partitionRecordsOrFail().forEach((topicPartition, records) -> {
            try {
                validateRecords(produceHar.getRequest().version(), records);
            } catch (ApiException ex) {
                invalidRequestResponses.put(topicPartition,
                        new ProduceResponse.PartitionResponse(Errors.forException(ex)));
                completeOne.run();
                return;
            }
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            authorize(AclOperation.WRITE, Resource.of(ResourceType.TOPIC, fullPartitionName))
                    .whenComplete((isAuthorized, ex) -> {
                        if (ex != null) {
                            log.error("Write topic authorize failed, topic - {}. {}",
                                    fullPartitionName, ex.getMessage());
                            unauthorizedTopicResponsesMap.put(topicPartition,
                                    new ProduceResponse.PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED));
                            completeOne.run();
                            return;
                        }
                        if (!isAuthorized) {
                            unauthorizedTopicResponsesMap.put(topicPartition,
                                    new ProduceResponse.PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED));
                            completeOne.run();
                            return;
                        }
                        authorizedRequestInfo.put(topicPartition, records);
                        completeOne.run();
                    });
        });


    }

    private void validateRecords(short version, MemoryRecords records) {
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
            throw new NotImplementedException("FindCoordinatorRequest not support unknown type "
                + request.coordinatorType());
        }

        String groupId = request.coordinatorKey();
        String groupIdPath = GroupIdUtils.groupIdPathFormat(findCoordinator.getClientHost(),
                findCoordinator.getHeader().clientId());

        // Store group name to metadata store for current client.
        storeGroupId(groupId, groupIdPath)
                .thenAccept(__ -> findBroker(TopicName.get(pulsarTopicName))
                        .thenAccept(node -> {
                            if (node.error() != Errors.NONE) {
                                log.error("[{}] Request {}: Error while find coordinator.",
                                        ctx.channel(), findCoordinator.getHeader());

                                resultFuture.complete(KafkaResponseUtils
                                        .newFindCoordinator(Errors.LEADER_NOT_AVAILABLE));
                                return;
                            }

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Found node {} as coordinator for key {} partition {}.",
                                        ctx.channel(), node.leader(), request.coordinatorKey(), partition);
                            }

                            resultFuture.complete(KafkaResponseUtils.newFindCoordinator(node.leader()));
                        }))
                .exceptionally(ex -> {
                    log.error("Store groupId failed.", ex);
                    return null;
                });
    }

    private CompletableFuture<Void> storeGroupId(String groupId, String groupIdPath) {
        String path = groupIdStoredPath + groupIdPath;
        CompletableFuture<Void> future = new CompletableFuture<>();
        metadataStore.put(path, groupId.getBytes(UTF_8), Optional.empty())
                .thenAccept(__ -> future.complete(null))
                .exceptionally(ex -> {
                    future.completeExceptionally(ex);
                    return null;
                });
        return future;
    }

    @VisibleForTesting
    public <T> void replaceTopicPartition(Map<TopicPartition, T> replacedMap,
                                          Map<TopicPartition, TopicPartition> replacingIndex) {
        String namespacePrefix = currentNamespacePrefix();
        Map<TopicPartition, T> newMap = new HashMap<>();
        replacedMap.entrySet().removeIf(entry -> {
            if (replacingIndex.containsKey(entry.getKey())) {
                newMap.put(replacingIndex.get(entry.getKey()), entry.getValue());
                return true;
            } else if (KopTopic.isFullTopicName(entry.getKey().topic())) {
                newMap.put(new TopicPartition(
                                KopTopic.removeDefaultNamespacePrefix(entry.getKey().topic(),
                                        namespacePrefix),
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
            final String namespacePrefix = currentNamespacePrefix();
            request.partitions().forEach(tp -> {
                try {
                    String fullName =  new KopTopic(tp.topic(), namespacePrefix).getFullName();
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

    private CompletableFuture<Pair<Errors, Long>> fetchOffset(String topicName, Long timestamp) {
        CompletableFuture<Pair<Errors, Long>> partitionData = new CompletableFuture<>();

        topicManager.getTopic(topicName).whenComplete((perTopicOpt, t) -> {
            if (t != null) {
                log.error("Failed while get persistentTopic topic: {} ts: {}. ",
                    !perTopicOpt.isPresent() ? "null" : perTopicOpt.get().getName(), timestamp, t);
                partitionData.complete(Pair.of(Errors.forException(t), null));
                return;
            }
            if (!perTopicOpt.isPresent()) {
                partitionData.complete(Pair.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, null));
                return;
            }
            PersistentTopic perTopic = perTopicOpt.get();
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) perTopic.getManagedLedger();
            PositionImpl lac = (PositionImpl) managedLedger.getLastConfirmedEntry();
            if (lac == null) {
                log.error("[{}] Unexpected LastConfirmedEntry for topic {}, managed ledger: {}",
                        ctx, perTopic.getName(), managedLedger.getName());
                partitionData.complete(Pair.of(Errors.UNKNOWN_SERVER_ERROR, -1L));
                return;
            }
            if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP) {
                PositionImpl position = (PositionImpl) managedLedger.getLastConfirmedEntry();
                if (log.isDebugEnabled()) {
                    log.debug("Get latest position for topic {} time {}. result: {}",
                        perTopic.getName(), timestamp, position);
                }
                long offset = MessageMetadataUtils.getLogEndOffset(managedLedger);
                partitionData.complete(Pair.of(Errors.NONE, offset));

            } else if (timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP) {
                PositionImpl position = OffsetFinder.getFirstValidPosition(managedLedger);
                if (position == null) {
                    log.error("[{}] Failed to find first valid position for topic {}", ctx, perTopic.getName());
                    partitionData.complete(Pair.of(Errors.UNKNOWN_SERVER_ERROR, -1L));
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Get earliest position for topic {}: {}, lac: {}",
                            ctx, perTopic.getName(), position, lac);
                }
                final long latestOffset = MessageMetadataUtils.getCurrentOffset(managedLedger);
                if (latestOffset < 0) {
                    log.warn("[{}] Unexpected latest offset {} (< 0) for topic {}",
                            ctx, latestOffset, perTopic.getName());
                    partitionData.complete(Pair.of(Errors.NONE, 0L));
                    return;
                }
                if (position.compareTo(lac) > 0) {
                    partitionData.complete(Pair.of(Errors.NONE, latestOffset));
                } else {
                    MessageMetadataUtils.getOffsetOfPosition(managedLedger, position, false,
                            timestamp, skipMessagesWithoutIndex)
                            .whenComplete((offset, throwable) -> {
                                if (throwable != null) {
                                    log.error("[{}] Failed to get offset for position {}",
                                            perTopic, position, throwable);
                                    partitionData.complete(Pair.of(Errors.UNKNOWN_SERVER_ERROR, null));
                                    return;
                                }
                                partitionData.complete(Pair.of(Errors.NONE, offset));
                    });
                }

            } else {
                fetchOffsetByTimestamp(partitionData, managedLedger, lac, timestamp, perTopic.getName());
            }
        });

        return partitionData;
    }

    private void fetchOffsetByTimestamp(CompletableFuture<Pair<Errors, Long>> partitionData,
                                        ManagedLedgerImpl managedLedger,
                                        PositionImpl lac,
                                        long timestamp,
                                        String topic) {
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
                                topic, timestamp);
                        partitionData.complete(Pair.of(Errors.UNKNOWN_SERVER_ERROR, null));
                        return;
                    }
                } else {
                    finalPosition = (PositionImpl) position;
                }


                if (log.isDebugEnabled()) {
                    log.debug("Find position for topic {} time {}. position: {}",
                            topic, timestamp, finalPosition);
                }

                if (finalPosition.compareTo(lac) > 0 || MessageMetadataUtils.getCurrentOffset(managedLedger) < 0) {
                    long offset = Math.max(0, MessageMetadataUtils.getCurrentOffset(managedLedger));
                    partitionData.complete(Pair.of(Errors.NONE, offset));
                } else {
                    MessageMetadataUtils.getOffsetOfPosition(managedLedger, finalPosition, true,
                            timestamp, skipMessagesWithoutIndex)
                            .whenComplete((offset, throwable) -> {
                                if (throwable != null) {
                                    log.error("[{}] Failed to get offset for position {}",
                                            topic, finalPosition, throwable);
                                    partitionData.complete(Pair.of(Errors.UNKNOWN_SERVER_ERROR, null));
                                    return;
                                }
                                partitionData.complete(Pair.of(Errors.NONE, offset));
                            });
                }
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception,
                                        Optional<Position> position, Object ctx) {
                log.warn("Unable to find position for topic {} time {}. Exception:",
                        topic, timestamp, exception);
                partitionData.complete(Pair.of(Errors.UNKNOWN_SERVER_ERROR, null));
            }
        });
    }

    private void waitResponseDataComplete(CompletableFuture<AbstractResponse> resultFuture,
                                          Map<TopicPartition, CompletableFuture<Pair<Errors, Long>>> responseData,
                                          boolean legacy) {
        CompletableFuture
                .allOf(responseData.values().toArray(new CompletableFuture<?>[0]))
                .whenComplete((ignore, ex) -> {
                    ListOffsetResponse response = KafkaResponseUtils.newListOffset(
                            CoreUtils.mapValue(responseData, CompletableFuture::join), legacy);
                    resultFuture.complete(response);
                });
    }

    private void handleListOffsetRequestV1AndAbove(KafkaHeaderAndRequest listOffset,
                                                   CompletableFuture<AbstractResponse> resultFuture) {
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();
        Map<TopicPartition, CompletableFuture<Pair<Errors, Long>>> responseData =
                Maps.newConcurrentMap();
        if (request.partitionTimestamps().size() == 0) {
            resultFuture.complete(new ListOffsetResponse(Collections.emptyMap()));
            return;
        }
        AtomicInteger partitions = new AtomicInteger(request.partitionTimestamps().size());
        Runnable completeOne = () -> {
            if (partitions.decrementAndGet() == 0) {
                waitResponseDataComplete(resultFuture, responseData, false);
            }
        };
        String namespacePrefix = currentNamespacePrefix();
        KafkaRequestUtils.forEachListOffsetRequest(request, (topic, times) -> {
            String fullPartitionName = KopTopic.toString(topic, namespacePrefix);
            authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, fullPartitionName))
                    .whenComplete((isAuthorized, ex) -> {
                                if (ex != null) {
                                    log.error("Describe topic authorize failed, topic - {}. {}",
                                            fullPartitionName, ex.getMessage());
                                    responseData.put(topic, CompletableFuture.completedFuture(
                                            Pair.of(Errors.TOPIC_AUTHORIZATION_FAILED, null)
                                    ));
                                    completeOne.run();
                                    return;
                                }
                                if (!isAuthorized) {
                                    responseData.put(topic, CompletableFuture.completedFuture(
                                            Pair.of(Errors.TOPIC_AUTHORIZATION_FAILED, null)
                                    ));
                                    completeOne.run();
                                    return;
                                }
                                responseData.put(topic, fetchOffset(fullPartitionName, times));
                                completeOne.run();
                            }
                    );

        });

    }

    // Some info can be found here
    // https://cfchou.github.io/blog/2015/04/23/a-closer-look-at-kafka-offsetrequest/ through web.archive.org
    private void handleListOffsetRequestV0(KafkaHeaderAndRequest listOffset,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        Map<TopicPartition, CompletableFuture<Pair<Errors, Long>>> responseData =
                Maps.newConcurrentMap();
        if (request.offsetData().size() == 0) {
            resultFuture.complete(new ListOffsetResponse(Collections.emptyMap()));
            return;
        }
        AtomicInteger partitions = new AtomicInteger(request.offsetData().size());
        Runnable completeOne = () -> {
            if (partitions.decrementAndGet() == 0) {
                waitResponseDataComplete(resultFuture, responseData, true);
            }
        };
        // in v0, the iterator is offsetData,
        // in v1, the iterator is partitionTimestamps,
        if (log.isDebugEnabled()) {
            log.debug("received a v0 listOffset: {}", request.toString(true));
        }
        String namespacePrefix = currentNamespacePrefix();
        KafkaRequestUtils.LegacyUtils.forEachListOffsetRequest(request, topic -> times -> maxNumOffsets -> {
            String fullPartitionName = KopTopic.toString(topic, namespacePrefix);

            authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, fullPartitionName))
                    .whenComplete((isAuthorized, ex) -> {
                        if (ex != null) {
                            log.error("Describe topic authorize failed, topic - {}. {}",
                                    fullPartitionName, ex.getMessage());
                            responseData.put(topic, CompletableFuture.completedFuture(
                                    Pair.of(Errors.TOPIC_AUTHORIZATION_FAILED, null)));
                            completeOne.run();
                            return;
                        }
                        if (!isAuthorized) {
                            responseData.put(topic, CompletableFuture.completedFuture(
                                    Pair.of(Errors.TOPIC_AUTHORIZATION_FAILED, null)));
                            completeOne.run();
                            return;
                        }

                        CompletableFuture<Pair<Errors, Long>> partitionData;
                        // num_num_offsets > 1 is not handled for now, returning an error
                        if (maxNumOffsets > 1) {
                            log.warn("request is asking for multiples offsets for {}, not supported for now",
                                    fullPartitionName);
                            partitionData = new CompletableFuture<>();
                            partitionData.complete(Pair.of(Errors.UNKNOWN_SERVER_ERROR, null));
                        }

                        partitionData = fetchOffset(fullPartitionName, times);
                        responseData.put(topic, partitionData);
                        completeOne.run();
                    });

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

        // V2 adds retention time to the request and V5 removes retention time
        long offsetRetention;
        if (apiVersion <= 1 || apiVersion >= 5 || retentionTime == OffsetCommitRequest.DEFAULT_RETENTION_TIME) {
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
            resultFuture.complete(KafkaResponseUtils.newOffsetCommit(Maps.newHashMap()));
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

                    OffsetCommitResponse response = KafkaResponseUtils.newOffsetCommit(offsetCommitResult);
                    resultFuture.complete(response);

                } else {
                    Map<TopicPartition, OffsetAndMetadata> convertedPartitionData =
                            convertOffsetCommitRequestRetentionMs(
                                    convertedOffsetData,
                                    KafkaRequestUtils.LegacyUtils.getRetentionTime(request),
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

                        OffsetCommitResponse response = KafkaResponseUtils.newOffsetCommit(offsetCommitResult);
                        resultFuture.complete(response);
                    });

                }
            }
        };
        final String namespacePrefix = currentNamespacePrefix();
        request.offsetData().forEach((tp, partitionData) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(tp.topic(), namespacePrefix);
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
                                    new KopTopic(tp.topic(), namespacePrefix).getFullName(), tp.partition());

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
        TransactionCoordinator transactionCoordinator = null;
        if (request.isolationLevel().equals(IsolationLevel.READ_COMMITTED)) {
            transactionCoordinator = getTransactionCoordinator();
        }
        String namespacePrefix = currentNamespacePrefix();
        MessageFetchContext.get(this, transactionCoordinator, fetch, resultFuture,
                fetchPurgatory, namespacePrefix).handleFetch();
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

            JoinGroupResponse response = KafkaResponseUtils.newJoinGroup(
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
            SyncGroupResponse response = KafkaResponseUtils.newSyncGroup(
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
            HeartbeatResponse response = KafkaResponseUtils.newHeartbeat(errors);

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
            resultFuture.complete(KafkaResponseUtils.newLeaveGroup(errors));
        });
    }

    @Override
    protected void handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(describeGroup.getRequest() instanceof DescribeGroupsRequest);
        DescribeGroupsRequest request = (DescribeGroupsRequest) describeGroup.getRequest();

        // let the coordinator to handle heartbeat
        resultFuture.complete(KafkaResponseUtils.newDescribeGroups(request.groupIds().stream()
                .map(groupId -> Pair.of(groupId, getGroupCoordinator().handleDescribeGroup(groupId)))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight))
        ));
    }

    @Override
    protected void handleListGroupsRequest(KafkaHeaderAndRequest listGroups,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(listGroups.getRequest() instanceof ListGroupsRequest);
        KeyValue<Errors, List<GroupOverview>> listResult = getGroupCoordinator().handleListGroups();
        resultFuture.complete(KafkaResponseUtils.newListGroups(listResult.getKey(), listResult.getValue()));
    }

    @Override
    protected void handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteGroups.getRequest() instanceof DeleteGroupsRequest);
        DeleteGroupsRequest request = (DeleteGroupsRequest) deleteGroups.getRequest();

        resultFuture.complete(KafkaResponseUtils.newDeleteGroups(
                getGroupCoordinator().handleDeleteGroups(request.groups())
        ));
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
        resultFuture.complete(KafkaResponseUtils.newSaslHandshake(Errors.ILLEGAL_SASL_STATE));
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
            resultFuture.complete(KafkaResponseUtils.newCreateTopics(result));
            return;
        }

        String namespacePrefix = currentNamespacePrefix();
        final AtomicInteger validTopicsCount = new AtomicInteger(validTopics.size());
        final Map<String, TopicDetails> authorizedTopics = Maps.newConcurrentMap();
        Runnable createTopicsAsync = () -> {
            if (authorizedTopics.isEmpty()) {
                resultFuture.complete(KafkaResponseUtils.newCreateTopics(result));
                return;
            }
            // TODO: handle request.validateOnly()
            adminManager.createTopicsAsync(authorizedTopics, request.timeout(), namespacePrefix)
                    .thenApply(validResult -> {
                result.putAll(validResult);
                resultFuture.complete(KafkaResponseUtils.newCreateTopics(result));
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
                kopTopic = new KopTopic(topic, namespacePrefix);
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
                           log.error("CreateTopics authorize failed, topic - {}.", fullTopicName);
                           completeOneErrorTopic
                                   .accept(topic, new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, null));
                           return;
                       }
                       completeOneTopic.accept(topic, details);
                    });
        });


    }

    @Override
    protected void handleAlterConfigs(KafkaHeaderAndRequest describeConfigs,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(describeConfigs.getRequest() instanceof AlterConfigsRequest);
        AlterConfigsRequest request = (AlterConfigsRequest) describeConfigs.getRequest();

        if (request.configs().isEmpty()) {
            resultFuture.complete(new AlterConfigsResponse(0, Maps.newHashMap()));
            return;
        }

        Map<ConfigResource, ApiError> results = new HashMap<>();
        request.configs().forEach((ConfigResource configResource, AlterConfigsRequest.Config newConfig) -> {
            newConfig.entries().forEach(entry -> {
                log.info("Ignoring ALTER_CONFIG for {} {} = {}", configResource, entry.name(), entry.value());
            });
            results.put(configResource, ApiError.NONE);
        });
        resultFuture.complete(new AlterConfigsResponse(0, results));
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

        String namespacePrefix = currentNamespacePrefix();
        Consumer<Runnable> completeOne = (action) -> {
            // When complete one authorization or failed, will do the action first.
            action.run();
            if (unfinishedAuthorizationCount.decrementAndGet() == 0) {
                adminManager.describeConfigsAsync(authorizedResources.stream()
                        .collect(Collectors.toMap(
                                resource -> resource,
                                resource -> Optional.ofNullable(request.configNames(resource)).map(HashSet::new)
                        )), namespacePrefix
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
                        kopTopic = new KopTopic(configResource.name(), namespacePrefix);
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
                    // but we are not exposing anything to the client, so it is fine to serve requests.
                    completeOne.accept(() -> authorizedResources.add(configResource));
                    break;
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
                request.transactionalId(), request.transactionTimeoutMs(), Optional.empty(), (resp) -> {
                    response.complete(
                            new InitProducerIdResponse(0, resp.getError(), resp.getProducerId(),
                                    resp.getProducerEpoch()));
                });
    }

    @Override
    protected void handleAddPartitionsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                            CompletableFuture<AbstractResponse> response) {
        AddPartitionsToTxnRequest request = (AddPartitionsToTxnRequest) kafkaHeaderAndRequest.getRequest();
        List<TopicPartition> partitionsToAdd = request.partitions();
        Map<TopicPartition, Errors> unauthorizedTopicErrors = Maps.newConcurrentMap();
        Map<TopicPartition, Errors> nonExistingTopicErrors = Maps.newConcurrentMap();
        Set<TopicPartition> authorizedPartitions = Sets.newConcurrentHashSet();
        TransactionCoordinator transactionCoordinator = getTransactionCoordinator();
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
                    transactionCoordinator.handleAddPartitionsToTransaction(request.transactionalId(),
                            request.producerId(), request.producerEpoch(), authorizedPartitions, (errors) -> {
                                // TODO: handle PRODUCER_FENCED errors
                                Map<TopicPartition, Errors> topicPartitionErrorsMap =
                                        addPartitionError(partitionsToAdd, errors);
                                response.complete(
                                        new AddPartitionsToTxnResponse(0, topicPartitionErrorsMap));
                            });
                }
            }
        };
        String namespacePrefix = currentNamespacePrefix();
        partitionsToAdd.forEach(tp -> {
            String fullPartitionName;
            try {
                fullPartitionName = KopTopic.toString(tp, namespacePrefix);
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
        Set<TopicPartition> topicPartitions = Collections.singleton(new TopicPartition(offsetTopicName, partition));
        transactionCoordinator.handleAddPartitionsToTransaction(
                request.transactionalId(),
                request.producerId(),
                request.producerEpoch(),
                topicPartitions,
                (errors) -> {
                    // TODO: handle PRODUCER_FENCED errors
                    response.complete(
                            new AddOffsetsToTxnResponse(0, errors));
                });
    }

    private Map<TopicPartition, Errors> addPartitionError(Collection<TopicPartition> partitions, Errors errors) {
        Map<TopicPartition, Errors> result = Maps.newHashMap();
        for (TopicPartition partition : partitions) {
            result.put(partition, errors);
        }
        return result;
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
        final String namespacePrefix = currentNamespacePrefix();
        request.offsets().forEach((tp, commitOffset) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(tp.topic(), namespacePrefix);
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
            String metadata = KafkaRequestUtils.getMetadata(partitionData);
            long offset = KafkaRequestUtils.getOffset(partitionData);
            offsetAndMetadataMap.put(entry.getKey(),
                    OffsetAndMetadata.apply(offset, metadata, currentTimestamp, -1));
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
                errors -> response.complete(new EndTxnResponse(0, errors)));
    }
    @Override
    protected void handleWriteTxnMarkers(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> response) {
        WriteTxnMarkersRequest request = (WriteTxnMarkersRequest) kafkaHeaderAndRequest.getRequest();

        Map<Long, Map<TopicPartition, Errors>> errors = new ConcurrentHashMap<>();
        List<WriteTxnMarkersRequest.TxnMarkerEntry> markers = request.markers();
        AtomicInteger numAppends = new AtomicInteger(markers.size());

        if (numAppends.get() == 0) {
            response.complete(new WriteTxnMarkersResponse(errors));
            return;
        }
        BiConsumer<Long, Map<TopicPartition, Errors>> updateErrors = (producerId, currentErrors) -> {
            Map<TopicPartition, Errors> previousErrors = errors.putIfAbsent(producerId, currentErrors);
            if (previousErrors != null) {
                previousErrors.putAll(currentErrors);
            }
        };
        Runnable completeOne = () -> {
          if (numAppends.decrementAndGet() == 0) {
              response.complete(new WriteTxnMarkersResponse(errors));
          }
        };

        for (WriteTxnMarkersRequest.TxnMarkerEntry marker : markers) {
            long producerId = marker.producerId();
            TransactionResult transactionResult = marker.transactionResult();
            Map<TopicPartition, MemoryRecords> controlRecords = generateTxnMarkerRecords(marker);
            AppendRecordsContext appendRecordsContext = AppendRecordsContext.get(
                    topicManager,
                    requestStats,
                    this::startSendOperationForThrottling,
                    this::completeSendOperationForThrottling,
                    this.pendingTopicFuturesMap);
            getReplicaManager().appendRecords(
                    kafkaConfig.getRequestTimeoutMs(),
                    true,
                    currentNamespacePrefix(),
                    controlRecords,
                    PartitionLog.AppendOrigin.Coordinator,
                    appendRecordsContext
            ).whenComplete((result, ex) -> {
                appendRecordsContext.recycle();
                if (ex != null) {
                    log.error("[{}] Append txn marker ({}) failed.", ctx.channel(), marker, ex);
                    Map<TopicPartition, Errors> currentErrors = new HashMap<>();
                    controlRecords.forEach(((topicPartition, partitionResponse) -> {
                        currentErrors.put(topicPartition, Errors.KAFKA_STORAGE_ERROR);
                    }));
                    updateErrors.accept(producerId, currentErrors);
                    completeOne.run();
                    return;
                }
                Map<TopicPartition, Errors> currentErrors = new HashMap<>();
                result.forEach(((topicPartition, partitionResponse) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Append txn marker to topic : [{}], response: [{}].",
                                ctx.channel(), topicPartition, partitionResponse);
                    }
                    currentErrors.put(topicPartition, partitionResponse.error);
                }));
                updateErrors.accept(producerId, currentErrors);

                final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();
                Set<TopicPartition> successfulOffsetsPartitions = result.keySet()
                        .stream()
                        .filter(topicPartition ->
                                KopTopic.isGroupMetadataTopicName(topicPartition.topic(), metadataNamespace))
                        .collect(Collectors.toSet());
                if (!successfulOffsetsPartitions.isEmpty()) {
                    getGroupCoordinator().scheduleHandleTxnCompletion(
                            producerId,
                            successfulOffsetsPartitions
                                    .stream().map(TopicPartition::partition).collect(Collectors.toSet()),
                            transactionResult).whenComplete((__, e) -> {
                                if (e != null) {
                                    log.error("Received an exception while trying to update the offsets cache on "
                                            + "transaction marker append", e);
                                    ConcurrentHashMap<TopicPartition, Errors> updatedErrors = new ConcurrentHashMap<>();
                                    successfulOffsetsPartitions.forEach(partition ->
                                            updatedErrors.put(partition, Errors.forException(e.getCause())));
                                    updateErrors.accept(producerId, updatedErrors);
                                }
                        completeOne.run();
                    });
                    return;
                }
                completeOne.run();
            });
        }
    }

    private Map<TopicPartition, MemoryRecords> generateTxnMarkerRecords(WriteTxnMarkersRequest.TxnMarkerEntry marker) {
        Map<TopicPartition, MemoryRecords> txnMarkerRecordsMap = Maps.newHashMap();

        ControlRecordType controlRecordType = marker.transactionResult().equals(TransactionResult.COMMIT)
                ? ControlRecordType.COMMIT : ControlRecordType.ABORT;
        EndTransactionMarker endTransactionMarker = new EndTransactionMarker(
                controlRecordType, marker.coordinatorEpoch());
        for (TopicPartition topicPartition : marker.partitions()) {
            MemoryRecords memoryRecords = MemoryRecords.withEndTransactionMarker(
                    marker.producerId(), marker.producerEpoch(), endTransactionMarker);
            txnMarkerRecordsMap.put(topicPartition, memoryRecords);
        }
        return txnMarkerRecordsMap;
    }

    @Override
    protected void handleDeleteTopics(KafkaHeaderAndRequest deleteTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteTopics.getRequest() instanceof DeleteTopicsRequest);
        DeleteTopicsRequest request = (DeleteTopicsRequest) deleteTopics.getRequest();
        Set<String> topicsToDelete = request.topics();
        if (topicsToDelete == null || topicsToDelete.isEmpty()) {
            resultFuture.complete(KafkaResponseUtils.newDeleteTopics(Maps.newHashMap()));
            return;
        }
        Map<String, Errors> deleteTopicsResponse = Maps.newConcurrentMap();
        AtomicInteger topicToDeleteCount = new AtomicInteger(topicsToDelete.size());
        BiConsumer<String, Errors> completeOne = (topic, errors) -> {
            deleteTopicsResponse.put(topic, errors);
            if (errors == Errors.NONE) {
                // create topic ZNode to trigger the coordinator DeleteTopicsEvent event
                metadataStore.put(
                        KopEventManager.getDeleteTopicsPath()
                                + "/" + TopicNameUtils.getTopicNameWithUrlEncoded(topic),
                        new byte[0],
                        Optional.empty());
            }
            if (topicToDeleteCount.decrementAndGet() == 0) {
                resultFuture.complete(KafkaResponseUtils.newDeleteTopics(deleteTopicsResponse));
            }
        };
        final String namespacePrefix = currentNamespacePrefix();
        topicsToDelete.forEach(topic -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(topic, namespacePrefix);
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
    protected void handleDeleteRecords(KafkaHeaderAndRequest deleteTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteTopics.getRequest() instanceof DeleteRecordsRequest);
        DeleteRecordsRequest request = (DeleteRecordsRequest) deleteTopics.getRequest();
        Map<TopicPartition, Long> partitionOffsets = request.partitionOffsets();
        if (partitionOffsets == null || partitionOffsets.isEmpty()) {
            resultFuture.complete(KafkaResponseUtils.newDeleteRecords(Maps.newHashMap()));
            return;
        }
        Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> deleteRecordsResponse =
                Maps.newConcurrentMap();
        AtomicInteger topicToDeleteCount = new AtomicInteger(partitionOffsets.size());
        BiConsumer<TopicPartition, Errors> completeOne = (topic, errors) -> {
            deleteRecordsResponse.put(topic, new DeleteRecordsResponse.PartitionResponse(0, errors));
            if (topicToDeleteCount.decrementAndGet() == 0) {
                resultFuture.complete(KafkaResponseUtils.newDeleteRecords(deleteRecordsResponse));
            }
        };
        final String namespacePrefix = currentNamespacePrefix();
        partitionOffsets.forEach((topicPartition, offset) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(topicPartition.topic(), namespacePrefix);
            } catch (KoPTopicException e) {
                completeOne.accept(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                return;
            }
            String fullTopicName = kopTopic.getPartitionName(topicPartition.partition());
            authorize(AclOperation.DELETE, Resource.of(ResourceType.TOPIC, fullTopicName))
                    .whenComplete((isAuthorize, ex) -> {
                        if (ex != null) {
                            log.error("DeleteTopics authorize failed, topic - {}. {}",
                                    fullTopicName, ex.getMessage());
                            completeOne.accept(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }
                        if (!isAuthorize) {
                            completeOne.accept(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }
                        topicManager
                            .getTopicConsumerManager(fullTopicName)
                                .thenAccept(topicManager -> {
                                    topicManager.findPositionForIndex(offset)
                                            .thenAccept(position -> {
                                                adminManager.truncateTopic(fullTopicName, offset, position,
                                                        __ -> completeOne.accept(topicPartition, Errors.NONE),
                                                        __ -> completeOne.accept(topicPartition,
                                                                Errors.UNKNOWN_TOPIC_OR_PARTITION));
                                            });
                                });

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

        KafkaRequestUtils.forEachCreatePartitionsRequest(request, (topic, newPartition) -> {
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
            resultFuture.complete(KafkaResponseUtils.newCreatePartitions(result));
            return;
        }

        String namespacePrefix = currentNamespacePrefix();
        final AtomicInteger validTopicsCount = new AtomicInteger(validTopics.size());
        final Map<String, NewPartitions> authorizedTopics = Maps.newConcurrentMap();
        Runnable createPartitionsAsync = () -> {
            if (authorizedTopics.isEmpty()) {
                resultFuture.complete(KafkaResponseUtils.newCreatePartitions(result));
                return;
            }
            adminManager.createPartitionsAsync(authorizedTopics, request.timeout(), namespacePrefix)
                    .thenApply(validResult -> {
                result.putAll(validResult);
                resultFuture.complete(KafkaResponseUtils.newCreatePartitions(result));
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
                KopTopic kopTopic = new KopTopic(topic, namespacePrefix);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        this.close();
    }

    // The returned future never completes exceptionally
    public CompletableFuture<PartitionMetadata> findBroker(TopicName topic) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Handle Lookup for {}", ctx.channel(), topic);
        }
        final CompletableFuture<PartitionMetadata> future = new CompletableFuture<>();
        kopBrokerLookupManager.findBroker(topic.toString(), advertisedEndPoint)
                .thenApply(listenerInetSocketAddressOpt -> listenerInetSocketAddressOpt
                        .map(inetSocketAddress -> newPartitionMetadata(topic, newNode(inetSocketAddress)))
                        .orElse(null)
                ).whenComplete((partitionMetadata, e) -> {
            if (e != null || partitionMetadata == null) {
                log.warn("[{}] Request {}: Exception while find Broker metadata", ctx.channel(), e);
                future.complete(newFailedPartitionMetadata(topic));
            } else {
                future.complete(partitionMetadata);
            }
        });
        return future;
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

    static PartitionMetadata newPartitionMetadata(TopicName topicName, Node node) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        if (log.isDebugEnabled()) {
            log.debug("Return PartitionMetadata node: {}, topicName: {}", node, topicName);
        }

        return KafkaResponseUtils.newMetadataPartition(kafkaPartitionIndex, node);
    }

    static PartitionMetadata newFailedPartitionMetadata(TopicName topicName) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        log.warn("Failed find Broker metadata, create PartitionMetadata with NOT_LEADER_FOR_PARTITION");

        // most of this error happens when topic is in loading/unloading status,
        return KafkaResponseUtils.newMetadataPartition(
                Errors.NOT_LEADER_FOR_PARTITION, kafkaPartitionIndex);
    }

    static AbstractResponse failedResponse(KafkaHeaderAndRequest requestHar, Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("Request {} get failed response ", requestHar.getHeader().apiKey(), e);
        }
        return requestHar.getRequest().getErrorResponse(((Integer) THROTTLE_TIME_MS.defaultValue), e);
    }

    private void throwIfTransactionCoordinatorDisabled() {
        if (!kafkaConfig.isKafkaTransactionCoordinatorEnabled()) {
            throw new IllegalArgumentException("Broker has disabled transaction coordinator, "
                    + "please enable it before using transaction.");
        }
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
                if (resource.getResourceType() == ResourceType.TOPIC) {
                    isAuthorizedFuture = authorizer.canLookupAsync(session.getPrincipal(), resource);
                } else if (resource.getResourceType() == ResourceType.NAMESPACE) {
                    isAuthorizedFuture = authorizer.canGetTopicList(session.getPrincipal(), resource);
                }
                break;
            case CREATE:
                isAuthorizedFuture = authorizer.canCreateTopicAsync(session.getPrincipal(), resource);
                break;
            case DELETE:
                isAuthorizedFuture = authorizer.canDeleteTopicAsync(session.getPrincipal(), resource);
                break;
            case ALTER:
                isAuthorizedFuture = authorizer.canAlterTopicAsync(session.getPrincipal(), resource);
                break;
            case DESCRIBE_CONFIGS:
                isAuthorizedFuture = authorizer.canManageTenantAsync(session.getPrincipal(), resource);
                break;
            case ANY:
                if (resource.getResourceType() == ResourceType.TENANT) {
                    isAuthorizedFuture = authorizer.canAccessTenantAsync(session.getPrincipal(), resource);
                }
                break;
            case ALTER_CONFIGS:
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
