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
package io.streamnative.kop;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.kop.coordinator.group.GroupMetadata.GroupSummary;
import io.streamnative.kop.offset.OffsetAndMetadata;
import io.streamnative.kop.utils.CoreUtils;
import io.streamnative.kop.utils.MessageIdUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMember;
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponse.PartitionData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
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
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * This class contains all the request handling methods.
 */
@Slf4j
@Getter
public class KafkaRequestHandler extends KafkaCommandDecoder {

    private final KafkaService kafkaService;
    private final String clusterName;
    private final NamespaceName kafkaNamespace;
    private final ExecutorService executor;
    private final PulsarAdmin admin;

    private static final Clock clock = Clock.systemDefaultZone();
    private static final String FAKE_KOP_PRODUCER_NAME = "fake_kop_producer_name";

    private static final int DEFAULT_FETCH_BUFFER_SIZE = 1024 * 1024;
    private static final int MAX_RECORDS_BUFFER_SIZE = 100 * 1024 * 1024;


    public KafkaRequestHandler(KafkaService kafkaService) throws Exception {
        super();
        this.kafkaService = kafkaService;

        this.clusterName = kafkaService.getKafkaConfig().getClusterName();
        this.kafkaNamespace = NamespaceName
            .get(kafkaService.getKafkaConfig().getKafkaTenant(),
                kafkaService.getKafkaConfig().getKafkaNamespace());
        this.executor = kafkaService.getExecutor();
        this.admin = kafkaService.getAdminClient();
    }

    protected CompletableFuture<ResponseAndRequest> handleApiVersionsRequest(KafkaHeaderAndRequest apiVersionRequest) {
        AbstractResponse apiResponse = ApiVersionsResponse.defaultApiVersionsResponse();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        resultFuture.complete(ResponseAndRequest.of(apiResponse, apiVersionRequest));
        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest) {
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();
        String err = String.format("Kafka API (%s) Not supported by kop server.",
            kafkaHeaderAndRequest.getHeader().apiKey());
        log.error(err);

        AbstractResponse apiResponse = kafkaHeaderAndRequest.getRequest()
            .getErrorResponse(new UnsupportedOperationException(err));
        resultFuture.complete(ResponseAndRequest.of(apiResponse, kafkaHeaderAndRequest));

        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleTopicMetadataRequest(KafkaHeaderAndRequest metadataHar) {
        checkArgument(metadataHar.getRequest() instanceof MetadataRequest);

        MetadataRequest metadataRequest = (MetadataRequest) metadataHar.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        // Command response for all topics
        List<TopicMetadata> allTopicMetadata = Collections.synchronizedList(Lists.newArrayList());
        List<Node> allNodes = Collections.synchronizedList(Lists.newArrayList());

        List<String> topics = metadataRequest.topics();
        // topics in format : persistent://%s/%s/abc-partition-x, will be grouped by as:
        //      Entry<abc, List[TopicName]>
        CompletableFuture<Map<String, List<TopicName>>> pulsarTopicsFuture = new CompletableFuture<>();

        // 1. get list of pulsarTopics
        if (topics == null || topics.isEmpty()) {
            try {
                Map<String, List<TopicName>> pulsarTopics =
                    kafkaService.getNamespaceService()
                        .getListOfPersistentTopics(kafkaNamespace)
                        .stream()
                        .map(topicString -> TopicName.get(topicString))
                        .collect(Collectors
                            .groupingBy(topicName -> getLocalNameWithoutPartition(topicName), Collectors.toList()));

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: Get all topics, will get {} topics",
                        ctx.channel(), metadataHar.getHeader(), pulsarTopics.size());
                }

                pulsarTopicsFuture.complete(pulsarTopics);
            } catch (Exception e) {
                // error when getListOfPersistentTopics
                log.error("[{}] Request {}: Failed to get all topics list",
                    ctx.channel(), metadataHar.getHeader(), e);
                pulsarTopicsFuture.completeExceptionally(e);
            }
        } else {
            Map<String, List<TopicName>> pulsarTopics = Maps.newHashMap();

            List<String> requestTopics = metadataRequest.topics();
            final int topicsNumber = requestTopics.size();
            AtomicInteger topicsCompleted = new AtomicInteger(0);

            requestTopics.stream()
                .forEach(topic -> {
                    TopicName topicName = pulsarTopicName(topic);
                    // get partition numbers for each topic.
                    PersistentTopicsBase
                        .getPartitionedTopicMetadata(
                            kafkaService,
                            null,
                            null,
                            null,
                            topicName)
                        .whenComplete((partitionedTopicMetadata, throwable) -> {
                            if (throwable != null) {
                                // Failed get partitions.
                                allTopicMetadata.add(
                                    new TopicMetadata(
                                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                        topic,
                                        false,
                                        Collections.emptyList()));
                                log.warn("[{}] Request {}: Failed to get partitioned topic {} metadata: {}",
                                    ctx.channel(), metadataHar.getHeader(), topicName, throwable.getMessage());
                            } else {
                                List<TopicName> topicNames;
                                if (partitionedTopicMetadata.partitions > 0) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Topic {} has {} partitions",
                                            topic, partitionedTopicMetadata.partitions);
                                    }
                                    topicNames = IntStream
                                        .range(0, partitionedTopicMetadata.partitions)
                                        .mapToObj(i ->
                                            TopicName.get(topicName.toString() + PARTITIONED_TOPIC_SUFFIX + i))
                                        .collect(Collectors.toList());
                                    pulsarTopics.put(topic, topicNames);
                                } else {
                                    if (kafkaService.getConfiguration().isAllowAutoTopicCreation()) {
                                        try {
                                            if (log.isDebugEnabled()) {
                                                log.debug("[{}] Request {}: Topic {} has single partition, "
                                                        + "auto create partitioned topic",
                                                    ctx.channel(), metadataHar.getHeader(), topic);
                                            }
                                            admin.topics().createPartitionedTopic(topicName.toString(), 1);
                                            pulsarTopics.put(topic, Lists.newArrayList(
                                                TopicName.get(topicName.toString() + PARTITIONED_TOPIC_SUFFIX + 0)));
                                        } catch (PulsarAdminException e) {
                                            log.error("[{}] Request {}: createPartitionedTopic failed.",
                                                ctx.channel(), metadataHar.getHeader(), e);
                                            allTopicMetadata.add(
                                                new TopicMetadata(
                                                    Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                    topic,
                                                    false,
                                                    Collections.emptyList()));
                                        }
                                    } else {
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Request {}: Topic {} has single partition, "
                                                    + "Not allow to auto create partitioned topic",
                                                ctx.channel(), metadataHar.getHeader(), topic);
                                        }
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
        pulsarTopicsFuture.whenComplete((pulsarTopics, e) -> {
            if (e != null) {
                log.warn("[{}] Request {}: Exception fetching metadata, will return null Response",
                    ctx.channel(), metadataHar.getHeader(), e);
                allNodes.add(newSelfNode());
                MetadataResponse finalResponse =
                    new MetadataResponse(
                        allNodes,
                        clusterName,
                        MetadataResponse.NO_CONTROLLER_ID,
                        Collections.emptyList());
                resultFuture.complete(ResponseAndRequest.of(finalResponse, metadataHar));
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
                        MetadataResponse.NO_CONTROLLER_ID,
                        allTopicMetadata);
                resultFuture.complete(ResponseAndRequest.of(finalResponse, metadataHar));
                return;
            }

            pulsarTopics.forEach((topic, list) -> {
                final int partitionsNumber = list.size();
                AtomicInteger partitionsCompleted = new AtomicInteger(0);
                List<PartitionMetadata> partitionMetadatas = Collections
                    .synchronizedList(Lists.newArrayListWithExpectedSize(partitionsNumber));

                list.forEach(topicName ->
                    findBroker(kafkaService, topicName)
                        .whenComplete(((partitionMetadata, throwable) -> {
                            if (throwable != null) {
                                log.warn("[{}] Request {}: Exception while find Broker metadata",
                                    ctx.channel(), metadataHar.getHeader(), throwable);
                                partitionMetadatas.add(newFailedPartitionMetadata(topicName));
                            } else {
                                Node newNode = partitionMetadata.leader();
                                if (!allNodes.stream().anyMatch(node1 -> node1.equals(newNode))) {
                                    allNodes.add(newNode);
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
                                    new TopicMetadata(Errors.NONE, topic, false, partitionMetadatas));

                                // whether completed all the topics requests.
                                int finishedTopics = topicsCompleted.incrementAndGet();
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Request {}: Completed findBroker for topic {}, "
                                            + "partitions found/all: {}/{}",
                                        ctx.channel(), metadataHar.getHeader(), topic,
                                        finishedTopics, topicsNumber);
                                }
                                if (finishedTopics == topicsNumber) {
                                    // TODO: confirm right value for controller_id
                                    MetadataResponse finalResponse =
                                        new MetadataResponse(
                                            allNodes,
                                            clusterName,
                                            MetadataResponse.NO_CONTROLLER_ID,
                                            allTopicMetadata);
                                    resultFuture.complete(ResponseAndRequest.of(finalResponse, metadataHar));
                                }
                            }
                        })));
            });
        });

        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleProduceRequest(KafkaHeaderAndRequest produceHar) {
        checkArgument(produceHar.getRequest() instanceof ProduceRequest);
        ProduceRequest produceRequest = (ProduceRequest) produceHar.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        if (produceRequest.transactionalId() != null) {
            log.warn("[{}] Transactions not supported", ctx.channel());

            resultFuture.complete(ResponseAndRequest.of(
                failedResponse(produceHar, new UnsupportedOperationException("No transaction support")),
                produceHar));
            return resultFuture;
        }

        // Ignore request.acks() and request.timeout(), which related to kafka replication in this broker.

        Map<TopicPartition, CompletableFuture<PartitionResponse>> responsesFutures = new HashMap<>();

        final int responsesSize = produceRequest.partitionRecordsOrFail().size();

        for (Map.Entry<TopicPartition, ? extends Records> entry : produceRequest.partitionRecordsOrFail().entrySet()) {
            TopicPartition topicPartition = entry.getKey();

            CompletableFuture<PartitionResponse> partitionResponse = new CompletableFuture<>();
            responsesFutures.put(topicPartition, partitionResponse);

            if (log.isDebugEnabled()) {
                log.debug("[{}] Request {}: Produce messages for topic {} partition {}, request size: {} ",
                    ctx.channel(), produceHar.getHeader(),
                    topicPartition.topic(), topicPartition.partition(), responsesSize);
            }

            TopicName topicName = pulsarTopicName(topicPartition);

            kafkaService.getBrokerService().getTopic(topicName.toString(), true)
                .whenComplete((topicOpt, exception) -> {
                    if (exception != null) {
                        log.error("[{}] Request {}: Failed to getOrCreateTopic {}. exception:",
                            ctx.channel(), produceHar.getHeader(), topicName, exception);
                        partitionResponse.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
                    } else {
                        if (topicOpt.isPresent()) {
                            publishMessages(entry.getValue(), topicOpt.get(), partitionResponse);
                        } else {
                            log.error("[{}] Request {}: getOrCreateTopic get empty topic for name {}",
                                ctx.channel(), produceHar.getHeader(), topicName);
                            partitionResponse.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
                        }
                    }
                });
        }

        CompletableFuture.allOf(responsesFutures.values().toArray(new CompletableFuture<?>[responsesSize]))
            .whenComplete((ignore, ex) -> {
                // all ex has translated to PartitionResponse with Errors.KAFKA_STORAGE_ERROR
                Map<TopicPartition, PartitionResponse> responses = new ConcurrentHashMap<>();
                for (Map.Entry<TopicPartition, CompletableFuture<PartitionResponse>> entry:
                    responsesFutures.entrySet()) {
                    responses.put(entry.getKey(), entry.getValue().join());
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: Complete handle produce.",
                        ctx.channel(), produceHar.toString());
                }
                resultFuture.complete(ResponseAndRequest.of(new ProduceResponse(responses), produceHar));
            });
        return resultFuture;
    }

    // publish Kafka records to pulsar topic, handle callback in MessagePublishContext.
    private void publishMessages(Records records,
                                 Topic topic,
                                 CompletableFuture<PartitionResponse> future) {

        // get records size.
        AtomicInteger size = new AtomicInteger(0);
        records.records().forEach(record -> size.incrementAndGet());
        int rec = size.get();

        if (log.isDebugEnabled()) {
            log.debug("[{}] publishMessages for topic partition: {} , records size is {} ",
                ctx.channel(), topic.getName(), size.get());
        }

        // TODO: Handle Records in a batched way:
        //      https://github.com/streamnative/kop/issues/16
        List<CompletableFuture<Long>> futures = Collections
            .synchronizedList(Lists.newArrayListWithExpectedSize(size.get()));

        records.records().forEach(record -> {
            CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
            futures.add(offsetFuture);
            ByteBuf headerAndPayload = messageToByteBuf(recordToEntry(record));
            topic.publishMessage(
                headerAndPayload,
                MessagePublishContext.get(
                    offsetFuture, topic, record.sequence(),
                    record.sizeInBytes(), System.nanoTime()));
        });

        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[rec])).whenComplete((ignore, ex) -> {
            if (ex != null) {
                log.error("[{}] publishMessages for topic partition: {} failed when write.",
                    ctx.channel(), topic.getName(), ex);
                future.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
            } else {
                future.complete(new PartitionResponse(Errors.NONE));
            }
        });
    }


    private static final class MessagePublishContext implements PublishContext {
        private CompletableFuture<Long> offsetFuture;
        private Topic topic;
        private long sequenceId;
        private int msgSize;
        private long startTimeNs;

        public long getSequenceId() {
            return sequenceId;
        }

        /**
         * Executed from managed ledger thread when the message is persisted.
         */
        @Override
        public void completed(Exception exception, long ledgerId, long entryId) {

            if (exception != null) {
                log.error("Failed write entry: ledgerId: {}, entryId: {}, sequenceId: {}. triggered send callback.",
                    ledgerId, entryId, sequenceId);
                offsetFuture.completeExceptionally(exception);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Success write topic: {}, ledgerId: {}, entryId: {}, sequenceId: {},"
                            + "messageSize: {}. And triggered send callback.",
                        topic.getName(), ledgerId, entryId, sequenceId, msgSize);
                }

                topic.recordAddLatency(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTimeNs));

                offsetFuture.complete(Long.valueOf(MessageIdUtils.getOffset(ledgerId, entryId)));
            }

            recycle();
        }

        // recycler
        static MessagePublishContext get(CompletableFuture<Long> offsetFuture,
                                         Topic topic,
                                         long sequenceId,
                                         int msgSize,
                                         long startTimeNs) {
            MessagePublishContext callback = RECYCLER.get();
            callback.offsetFuture = offsetFuture;
            callback.topic = topic;
            callback.sequenceId = sequenceId;
            callback.msgSize = msgSize;
            callback.startTimeNs = startTimeNs;
            return callback;
        }

        private final Handle<MessagePublishContext> recyclerHandle;

        private MessagePublishContext(Handle<MessagePublishContext> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
            protected MessagePublishContext newObject(Recycler.Handle<MessagePublishContext> handle) {
                return new MessagePublishContext(handle);
            }
        };

        public void recycle() {
            offsetFuture = null;
            topic = null;
            sequenceId = -1;
            msgSize = 0;
            startTimeNs = -1;
            recyclerHandle.recycle(this);
        }
    }

    // convert kafka Record to Pulsar Message.
    private Message<byte[]> recordToEntry(Record record) {
        @SuppressWarnings("unchecked")
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl(null, Schema.BYTES);

        // key
        if (record.hasKey()) {
            byte[] key = new byte[record.keySize()];
            record.key().get(key);
            builder.keyBytes(key);
        }

        // value
        if (record.hasValue()) {
            byte[] value = new byte[record.valueSize()];
            record.value().get(value);
            builder.value(value);
        } else {
            builder.value(new byte[0]);
        }

        // sequence
        if (record.sequence() >= 0) {
            builder.sequenceId(record.sequence());
        }

        // timestamp
        if (record.timestamp() >= 0) {
            builder.eventTime(record.timestamp());
        }

        // header
        for (Header h : record.headers()) {
            builder.property(h.key(),
                Base64.getEncoder().encodeToString(h.value()));
        }

        return builder.getMessage();
    }

    // convert message to ByteBuf payload for ledger.addEntry.
    private ByteBuf messageToByteBuf(Message<byte[]> message) {
        checkArgument(message instanceof MessageImpl);

        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) message;
        MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // filled in required fields
        if (!msgMetadataBuilder.hasSequenceId()) {
            msgMetadataBuilder.setSequenceId(-1);
        }
        if (!msgMetadataBuilder.hasPublishTime()) {
            msgMetadataBuilder.setPublishTime(clock.millis());
        }
        if (!msgMetadataBuilder.hasProducerName()) {
            msgMetadataBuilder.setProducerName(FAKE_KOP_PRODUCER_NAME);
        }

        msgMetadataBuilder.setCompression(
            CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        msgMetadataBuilder.setUncompressedSize(payload.readableBytes());
        MessageMetadata msgMetadata = msgMetadataBuilder.build();

        ByteBuf buf = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, msgMetadata, payload);

        msgMetadataBuilder.recycle();
        msgMetadata.recycle();

        return buf;
    }


    // convert entries read from BookKeeper into Kafka Records
    private static MemoryRecords entriesToRecords(List<Entry> entries) {
        try (ByteBufferOutputStream outputStream = new ByteBufferOutputStream(DEFAULT_FETCH_BUFFER_SIZE)) {
            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(outputStream, RecordBatch.CURRENT_MAGIC_VALUE,
                org.apache.kafka.common.record.CompressionType.NONE,
                TimestampType.CREATE_TIME,
                MessageIdUtils.getOffset(entries.get(0).getLedgerId(), 0),
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                MAX_RECORDS_BUFFER_SIZE);

            for (Entry entry : entries) {
                ByteBuf metadataAndPayload = entry.getDataBuffer();
                MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);
                ByteBuf payload = metadataAndPayload.retain();

                byte[] data = new byte[payload.readableBytes()];
                payload.readBytes(data);

                builder.appendWithOffset(
                    MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId()),
                    msgMetadata.getEventTime(),
                    Base64.getDecoder().decode(msgMetadata.getPartitionKey()),
                    data);
            }
            return builder.build();
        } catch (IOException ioe) {
            log.error("Meet IOException: {}", ioe);
            throw new UncheckedIOException(ioe);
        } catch (Exception e) {
            log.error("Meet exception: {}", e);
            throw e;
        }
    }

    // A simple implementation, returns this broker node.
    protected CompletableFuture<ResponseAndRequest>
    handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator) {
        checkArgument(findCoordinator.getRequest() instanceof FindCoordinatorRequest);
        FindCoordinatorRequest request = (FindCoordinatorRequest) findCoordinator.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        if (request.coordinatorType() == FindCoordinatorRequest.CoordinatorType.GROUP) {
            AbstractResponse response;
            try {
                URI uri = new URI(kafkaService.getBrokerServiceUrl());
                Node node = newNode(
                    new InetSocketAddress(
                        uri.getHost(),
                        kafkaService.getKafkaConfig().getKafkaServicePort().get()));

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: Return current broker node as Coordinator: {}.",
                        ctx.channel(), findCoordinator.getHeader(), node);
                }

                response = new FindCoordinatorResponse(
                    Errors.NONE,
                    node);
            } catch (Exception e) {
                log.error("[{}] Request {}: Error while find coordinator.",
                    ctx.channel(), findCoordinator.getHeader(), e);
                response = new FindCoordinatorResponse(
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    Node.noNode());
            }

            resultFuture.complete(ResponseAndRequest.of(response, findCoordinator));
        } else {
            throw new NotImplementedException("FindCoordinatorRequest not support TRANSACTION type");
        }

        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch) {
        checkArgument(offsetFetch.getRequest() instanceof OffsetFetchRequest);
        OffsetFetchRequest request = (OffsetFetchRequest) offsetFetch.getRequest();
        checkState(kafkaService.getGroupCoordinator() != null,
            "Group Coordinator not started");
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        KeyValue<Errors, Map<TopicPartition, OffsetFetchResponse.PartitionData>> keyValue =
            kafkaService.getGroupCoordinator().handleFetchOffsets(
                request.groupId(),
                Optional.of(request.partitions())
            );

        resultFuture.complete(ResponseAndRequest
            .of(new OffsetFetchResponse(keyValue.getKey(), keyValue.getValue()), offsetFetch));

        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleListOffsetRequest(KafkaHeaderAndRequest listOffset) {
        checkArgument(listOffset.getRequest() instanceof ListOffsetRequest);
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        List<Pair<TopicPartition, CompletableFuture<PersistentTopicInternalStats>>> statsList =
            request.partitionTimestamps().entrySet().stream()
                .map((topicPartition) -> Pair.of(topicPartition.getKey(), admin.topics()
                    .getInternalStatsAsync(pulsarTopicName(topicPartition.getKey()).toString())))
                .collect(Collectors.toList());

        CompletableFuture.allOf(statsList.stream().map(entry -> entry.getValue()).toArray(CompletableFuture<?>[]::new))
            .whenComplete((ignore, ex) -> {
                Map<TopicPartition, ListOffsetResponse.PartitionData> responses =
                    statsList.stream().map((v) -> {
                        TopicPartition key = v.getKey();
                        try {
                            List<PersistentTopicInternalStats.LedgerInfo> ledgers = v.getValue().join().ledgers;
                            // return first ledger.
                            long offset = MessageIdUtils.getOffset(ledgers.get(0).ledgerId, 0);

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Request {}:  topic {}  Return offset: {}, ledgerId: {}.",
                                    ctx.channel(), listOffset.getHeader(), key, offset, ledgers.get(0).ledgerId);
                            }
                            return Pair.of(key, new ListOffsetResponse.PartitionData(
                                Errors.NONE, -1L, offset));
                        } catch (Exception e) {
                            log.error("[{}] Request {}: topic {} meet error of getInternalStats.",
                                ctx.channel(), listOffset.getHeader(), key, e);
                            return Pair.of(key, new ListOffsetResponse.PartitionData(
                                Errors.UNKNOWN_TOPIC_OR_PARTITION, -1L, -1L));
                        }
                    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

                resultFuture.complete(ResponseAndRequest
                    .of(new ListOffsetResponse(responses), listOffset));
            });

        return resultFuture;
    }

    // For non exist topics handleOffsetCommitRequest return UNKNOWN_TOPIC_OR_PARTITION
    private Map<TopicPartition, Errors> nonExistingTopicErrors(OffsetCommitRequest request) {
        return request.offsetData().entrySet().stream()
                .filter(entry ->
                    // filter not exist topics
                    !kafkaService.getKafkaTopicManager()
                        .topicExists(pulsarTopicName(entry.getKey()).toString()))
                .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> Errors.UNKNOWN_TOPIC_OR_PARTITION
                ));
    }

    protected CompletableFuture<ResponseAndRequest> handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit) {
        checkArgument(offsetCommit.getRequest() instanceof OffsetCommitRequest);
        checkState(kafkaService.getGroupCoordinator() != null,
            "Group Coordinator not started");

        OffsetCommitRequest request = (OffsetCommitRequest) offsetCommit.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        Map<TopicPartition, Errors> nonExistingTopic = nonExistingTopicErrors(request);

        kafkaService.getGroupCoordinator().handleCommitOffsets(
            request.groupId(),
            request.memberId(),
            request.generationId(),
            CoreUtils.mapValue(
                request.offsetData(),
                (partitionData) ->
                    OffsetAndMetadata.apply(partitionData.offset, partitionData.metadata, partitionData.timestamp)
            )
        ).thenAccept(offsetCommitResult -> {
            if (nonExistingTopic != null) {
                offsetCommitResult.putAll(nonExistingTopic);
            }
            OffsetCommitResponse response = new OffsetCommitResponse(offsetCommitResult);
            resultFuture.complete(ResponseAndRequest.of(response, offsetCommit));
        });

        return resultFuture;
    }

    private void readMessages(KafkaHeaderAndRequest fetch,
                              Map<TopicPartition, CompletableFuture<Pair<ManagedCursor, Long>>> cursors,
                              CompletableFuture<ResponseAndRequest> resultFuture) {
        AtomicInteger bytesRead = new AtomicInteger(0);
        Map<TopicPartition, List<Entry>> responseValues = new ConcurrentHashMap<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Request {}: Read Messages for request.",
                ctx.channel(), fetch.getHeader());
        }

        readMessagesInternal(fetch, cursors, bytesRead, responseValues, resultFuture);
    }

    private void readMessagesInternal(KafkaHeaderAndRequest fetch,
                                      Map<TopicPartition, CompletableFuture<Pair<ManagedCursor, Long>>> cursors,
                                      AtomicInteger bytesRead,
                                      Map<TopicPartition, List<Entry>> responseValues,
                                      CompletableFuture<ResponseAndRequest> resultFuture) {
        AtomicInteger entriesRead = new AtomicInteger(0);
        Map<TopicPartition, CompletableFuture<Entry>> readFutures = readAllCursorOnce(cursors);
        CompletableFuture.allOf(readFutures.values().stream().toArray(CompletableFuture<?>[]::new))
            .whenComplete((ignore, ex) -> {
                // keep entries since all read completed.
                readFutures.forEach((topic, readEntry) -> {
                    try {
                        Entry entry = readEntry.join();
                        List<Entry> entryList = responseValues.computeIfAbsent(topic, l -> Lists.newArrayList());

                        if (entry != null) {
                            entryList.add(entry);
                            entriesRead.incrementAndGet();
                            bytesRead.addAndGet(entry.getLength());
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Request {}: For topic {}, entries in list: {}. add new entry {}:{}",
                                    ctx.channel(), fetch.getHeader(), topic.toString(), entryList.size(),
                                    entry.getLedgerId(), entry.getEntryId());
                            }
                        }
                    } catch (Exception e) {
                        // readEntry.join failed. ignore this partition
                        log.error("[{}] Request {}: Failed readEntry.join for topic: {}. ",
                            ctx.channel(), fetch.getHeader(), topic, e);
                        cursors.remove(topic);
                        responseValues.putIfAbsent(topic, Lists.newArrayList());
                    }
                });

                FetchRequest request = (FetchRequest) fetch.getRequest();
                int maxBytes = request.maxBytes();
                int minBytes = request.minBytes();
                int waitTime = request.maxWait(); // in ms
                // if endTime <= 0, then no time wait, wait for minBytes.
                long endTime = waitTime > 0 ? System.currentTimeMillis() + waitTime : waitTime;

                int allSize = bytesRead.get();

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: One round read {} entries, "
                            + "allSize/maxBytes/minBytes/endTime: {}/{}/{}/{}",
                        ctx.channel(), fetch.getHeader(), entriesRead.get(),
                        allSize, maxBytes, minBytes, new Date(endTime));
                }

                // all partitions read no entry, return earlier;
                // reach maxTime, return;
                // reach minBytes if no endTime, return;
                if ((allSize == 0 && entriesRead.get() == 0)
                    || (endTime > 0 && endTime <= System.currentTimeMillis())
                    || allSize > minBytes
                    || allSize > maxBytes){
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Request {}: Complete read {} entries with size {}",
                            ctx.channel(), fetch.getHeader(), entriesRead.get(), allSize);
                    }

                    LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();

                    AtomicBoolean allPartitionsNoEntry = new AtomicBoolean(true);
                    responseValues.forEach((topicPartition, entries) -> {
                        final FetchResponse.PartitionData partitionData;
                        if (entries.isEmpty()) {
                            partitionData = new FetchResponse.PartitionData(
                                Errors.NONE,
                                FetchResponse.INVALID_HIGHWATERMARK,
                                FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                FetchResponse.INVALID_LOG_START_OFFSET,
                                null,
                                MemoryRecords.EMPTY);
                        } else {
                            allPartitionsNoEntry.set(false);
                            Entry entry = entries.get(entries.size() - 1);
                            long entryOffset = MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId());
                            long highWatermark = entryOffset
                                + cursors.get(topicPartition).join().getLeft().getNumberOfEntries();

                            MemoryRecords records = entriesToRecords(entries);
                            partitionData = new FetchResponse.PartitionData(
                                Errors.NONE,
                                highWatermark,
                                highWatermark,
                                highWatermark,
                                null,
                                records);
                        }
                        responseData.put(topicPartition, partitionData);
                    });

                    if (allPartitionsNoEntry.get()) {
                        log.error("[{}] Request {}: All partitions for request read 0 entry",
                            ctx.channel(), fetch.getHeader());

                        // returned earlier, sleep for waitTime
                        try {
                            Thread.sleep(waitTime);
                        } catch (Exception e) {
                            log.error("[{}] Request {}: error while sleep.",
                                ctx.channel(), fetch.getHeader(), e);
                        }

                        resultFuture.complete(ResponseAndRequest.of(
                            new FetchResponse(Errors.NONE,
                                responseData,
                                ((Integer) THROTTLE_TIME_MS.defaultValue),
                                ((FetchRequest) fetch.getRequest()).metadata().sessionId()),
                            fetch));
                    } else {
                        resultFuture.complete(ResponseAndRequest.of(
                            new FetchResponse(
                                Errors.NONE,
                                responseData,
                                ((Integer) THROTTLE_TIME_MS.defaultValue),
                                ((FetchRequest) fetch.getRequest()).metadata().sessionId()),
                            fetch));
                    }
                } else {
                    //need do another round read
                    readMessagesInternal(fetch, cursors, bytesRead, responseValues, resultFuture);
                }
            });
    }

    private Map<TopicPartition, CompletableFuture<Entry>> readAllCursorOnce(
            Map<TopicPartition, CompletableFuture<Pair<ManagedCursor, Long>>> cursors) {
        Map<TopicPartition, CompletableFuture<Entry>> readFutures = new ConcurrentHashMap<>();

        cursors.entrySet().forEach(pair -> {
            // non durable cursor create is a sync method.
            ManagedCursor cursor;
            CompletableFuture<Entry> readFuture = new CompletableFuture<>();

            try {
                Pair<ManagedCursor, Long> cursorOffsetPair = pair.getValue().join();
                cursor = cursorOffsetPair.getLeft();
                long keptOffset = cursorOffsetPair.getRight();

                // only read 1 entry currently. could read more in a batch.
                cursor.asyncReadEntries(1,
                    new ReadEntriesCallback() {
                        @Override
                        public void readEntriesComplete(List<Entry> list, Object o) {
                            TopicName topicName = pulsarTopicName(pair.getKey());

                            Entry entry = null;
                            if (!list.isEmpty()) {
                                entry = list.get(0);
                                long offset = MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId());

                                if (log.isDebugEnabled()) {
                                    log.debug("Topic {} success read entry: ledgerId: {}, entryId: {}, size: {},"
                                            + " ConsumerManager original offset: {}, entryOffset: {}",
                                        topicName.toString(), entry.getLedgerId(), entry.getEntryId(),
                                        entry.getLength(), keptOffset, offset);
                                }

                                kafkaService.getKafkaTopicManager()
                                    .getTopicConsumerManager(topicName.toString())
                                    .thenAccept(cm -> cm.add(offset + 1, Pair.of(cursor, offset + 1)));
                            } else {
                                // since no read entry, add the original offset back.
                                if (log.isDebugEnabled()) {
                                    log.debug("Read no entry, add offset back:  {}",
                                        keptOffset);
                                }

                                kafkaService.getKafkaTopicManager()
                                    .getTopicConsumerManager(topicName.toString())
                                    .thenAccept(cm ->
                                        cm.add(keptOffset, Pair.of(cursor, keptOffset)));
                            }

                            readFuture.complete(entry);
                        }

                        @Override
                        public void readEntriesFailed(ManagedLedgerException e, Object o) {
                            log.error("Error read entry for topic: {}", pulsarTopicName(pair.getKey()));
                            readFuture.completeExceptionally(e);
                        }
                    }, null);
            } catch (Exception e) {
                log.error("Error for cursor to read entry for topic: {}. ", pulsarTopicName(pair.getKey()), e);
                readFuture.completeExceptionally(e);
            }

            readFutures.putIfAbsent(pair.getKey(), readFuture);
        });

        return readFutures;
    }


    protected CompletableFuture<ResponseAndRequest> handleFetchRequest(KafkaHeaderAndRequest fetch) {
        checkArgument(fetch.getRequest() instanceof FetchRequest);
        FetchRequest request = (FetchRequest) fetch.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Request {} Fetch request. Size: {}. Each item: ",
                ctx.channel(), fetch.getHeader(), request.fetchData().size());

            request.fetchData().forEach((topic, data) -> {
                log.debug("  Fetch request topic:{} data:{}.",
                    topic, data.toString());
            });
        }

        // Map of partition and related cursor
        Map<TopicPartition, CompletableFuture<Pair<ManagedCursor, Long>>> topicsAndCursor = request
            .fetchData().entrySet().stream()
            .map(entry -> {
                TopicName topicName = pulsarTopicName(entry.getKey());
                long offset = entry.getValue().fetchOffset;

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: Fetch topic {}, remove cursor for fetch offset: {}.",
                        ctx.channel(), fetch.getHeader(), topicName, offset);
                }

                return Pair.of(
                    entry.getKey(),
                    kafkaService.getKafkaTopicManager()
                        .getTopicConsumerManager(topicName.toString())
                        .thenCompose(cm -> cm.remove(offset)));
            })
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // wait to get all the cursor, then readMessages
        CompletableFuture
            .allOf(topicsAndCursor.entrySet().stream().map(Map.Entry::getValue).toArray(CompletableFuture<?>[]::new))
            .whenComplete((ignore, ex) -> readMessages(fetch, topicsAndCursor, resultFuture));

        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup) {
        checkArgument(joinGroup.getRequest() instanceof JoinGroupRequest);
        checkState(kafkaService.getGroupCoordinator() != null,
            "Group Coordinator not started");

        JoinGroupRequest request = (JoinGroupRequest) joinGroup.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        Map<String, byte[]> protocols = new HashMap<>();
        request.groupProtocols()
            .stream()
            .forEach(protocol -> protocols.put(protocol.name(), Utils.toArray(protocol.metadata())));
        kafkaService.getGroupCoordinator().handleJoinGroup(
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

            resultFuture.complete(ResponseAndRequest.of(response, joinGroup));
        });

        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup) {
        checkArgument(syncGroup.getRequest() instanceof SyncGroupRequest);
        SyncGroupRequest request = (SyncGroupRequest) syncGroup.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        kafkaService.getGroupCoordinator().handleSyncGroup(
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

            resultFuture.complete(ResponseAndRequest.of(response, syncGroup));
        });

        return resultFuture;
    }

    protected CompletableFuture<ResponseAndRequest> handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat) {
        checkArgument(heartbeat.getRequest() instanceof HeartbeatRequest);
        HeartbeatRequest request = (HeartbeatRequest) heartbeat.getRequest();

        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        // let the coordinator to handle heartbeat
        kafkaService.getGroupCoordinator().handleHeartbeat(
            request.groupId(),
            request.memberId(),
            request.groupGenerationId()
        ).thenAccept(errors -> {
            HeartbeatResponse response = new HeartbeatResponse(errors);

            if (log.isTraceEnabled()) {
                log.trace("Sending heartbeat response {} for correlation id {} to client {}.",
                    response, heartbeat.getHeader().correlationId(), heartbeat.getHeader().clientId());
            }

            resultFuture.complete(ResponseAndRequest.of(response, heartbeat));
        });
        return resultFuture;
    }

    @Override
    protected CompletableFuture<ResponseAndRequest> handleLeaveGroupRequest(KafkaHeaderAndRequest leaveGroup) {
        checkArgument(leaveGroup.getRequest() instanceof LeaveGroupRequest);
        LeaveGroupRequest request = (LeaveGroupRequest) leaveGroup.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        // let the coordinator to handle heartbeat
        kafkaService.getGroupCoordinator().handleLeaveGroup(
            request.groupId(),
            request.memberId()
        ).thenAccept(errors -> {
            LeaveGroupResponse response = new LeaveGroupResponse(errors);

            resultFuture.complete(ResponseAndRequest.of(response, leaveGroup));
        });

        return resultFuture;
    }

    @Override
    protected CompletableFuture<ResponseAndRequest> handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup) {
        checkArgument(describeGroup.getRequest() instanceof DescribeGroupsRequest);
        DescribeGroupsRequest request = (DescribeGroupsRequest) describeGroup.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        // let the coordinator to handle heartbeat
        Map<String, GroupMetadata> groups = request.groupIds().stream()
            .map(groupId -> {
                KeyValue<Errors, GroupSummary> describeResult = kafkaService.getGroupCoordinator()
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
        resultFuture.complete(ResponseAndRequest.of(response, describeGroup));

        return resultFuture;
    }

    @Override
    protected CompletableFuture<ResponseAndRequest> handleListGroupsRequest(KafkaHeaderAndRequest listGroups) {
        throw new NotImplementedException("Not implemented yet");
    }

    @Override
    protected CompletableFuture<ResponseAndRequest> handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups) {
        checkArgument(deleteGroups.getRequest() instanceof DescribeGroupsRequest);
        DeleteGroupsRequest request = (DeleteGroupsRequest) deleteGroups.getRequest();
        CompletableFuture<ResponseAndRequest> resultFuture = new CompletableFuture<>();

        Map<String, Errors> deleteResult = kafkaService.getGroupCoordinator().handleDeleteGroups(request.groups());
        DeleteGroupsResponse response = new DeleteGroupsResponse(
            deleteResult
        );
        resultFuture.complete(ResponseAndRequest.of(response, deleteGroups));
        return resultFuture;
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        ctx.close();
    }

    private CompletableFuture<PartitionMetadata> findBroker(KafkaService kafkaService, TopicName topic) {
        if (log.isDebugEnabled()) {
            log.debug("Handle Lookup for {}", topic);
        }

        final CompletableFuture<PartitionMetadata> resultFuture = new CompletableFuture<>();

        kafkaService.getNamespaceService()
            .getBrokerServiceUrlAsync(topic, true)
            .whenComplete((lookupResult, throwable)-> {
                if (throwable != null) {
                    log.error("Caught error while find Broker for topic:{} ", topic, throwable);
                    resultFuture.completeExceptionally(throwable);
                    return;
                }

                try {
                    if (lookupResult.isPresent()) {
                        LookupData lookupData = lookupResult.get().getLookupData();
                        String brokerUrl = lookupData.getBrokerUrl();

                        URI uri = new URI(brokerUrl);
                        if (log.isDebugEnabled()) {
                            log.debug("Found broker: {} for topicName: {}", uri, topic);
                        }

                        Node node = newNode(new InetSocketAddress(
                            uri.getHost(),
                            kafkaService.getKafkaConfig().getKafkaServicePort().get()));

                        resultFuture.complete(newPartitionMetadata(topic, node));
                        return;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Topic {} not owned by broker.", topic);
                        }
                        resultFuture.complete(newFailedPartitionMetadata(topic));
                        return;
                    }
                } catch (Exception e) {
                    log.error("Caught error while find Broker for topic:{} ", topic, e);
                    resultFuture.completeExceptionally(e);
                }
            });

        return resultFuture;
    }

    private TopicName pulsarTopicName(TopicPartition topicPartition) {
        return pulsarTopicName(topicPartition.topic(), topicPartition.partition());
    }

    private TopicName pulsarTopicName(String topic) {
        return TopicName.get(TopicDomain.persistent.value(), kafkaNamespace, topic);
    }

    private TopicName pulsarTopicName(String topic, int partitionIndex) {
        return TopicName.get(TopicDomain.persistent.value(),
            kafkaNamespace,
            topic + PARTITIONED_TOPIC_SUFFIX + partitionIndex);
    }

    // TODO: handle Kafka Node.id
    //   - https://github.com/streamnative/kop/issues/9
    static Node newNode(InetSocketAddress address) {
        if (log.isDebugEnabled()) {
            log.debug("Return Broker Node of {}", address);
        }
        return new Node(
            Murmur3_32Hash.getInstance().makeHash((address.getHostString() + address.getPort()).getBytes(UTF_8)),
            address.getHostString(),
            address.getPort());
    }

    Node newSelfNode() {
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(
            kafkaService.getKafkaConfig().getAdvertisedAddress());
        int port = kafkaService.getKafkaConfig().getKafkaServicePort().get();

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

        log.warn("Failed find Broker metadata, create PartitionMetadata with INVALID_PARTITIONS");

        return new PartitionMetadata(
            Errors.UNKNOWN_SERVER_ERROR,
            kafkaPartitionIndex,
            Node.noNode(),                      // leader
            Lists.newArrayList(Node.noNode()),  // replicas
            Lists.newArrayList(Node.noNode()),  // isr
            Collections.emptyList()             // offline replicas
        );
    }

    static String getLocalNameWithoutPartition(TopicName topicName) {
        String localName = topicName.getLocalName();
        if (localName.contains(PARTITIONED_TOPIC_SUFFIX)) {
            return localName.substring(0, localName.lastIndexOf(PARTITIONED_TOPIC_SUFFIX));
        } else {
            return localName;
        }
    }

    static AbstractResponse failedResponse(KafkaHeaderAndRequest requestHar, Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("Request {} get failed response ", requestHar.getHeader().apiKey(), e);
        }
        return requestHar.getRequest().getErrorResponse(((Integer) THROTTLE_TIME_MS.defaultValue), e);
    }
}
