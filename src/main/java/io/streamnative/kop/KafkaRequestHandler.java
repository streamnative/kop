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
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.kop.utils.MessageIdUtils;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
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
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;

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
    private static final String producerName = "fake_kop_producer_name";


    public KafkaRequestHandler(KafkaService kafkaService) throws Exception {
        super();
        this.kafkaService = kafkaService;

        this.clusterName = kafkaService.getKafkaConfig().getClusterName();
        this.kafkaNamespace = NamespaceName.get(kafkaService.getKafkaConfig().getKafkaNamespace());
        this.executor = kafkaService.getExecutor();
        this.admin = kafkaService.getAdminClient();
    }

    protected void handleApiVersionsRequest(KafkaHeaderAndRequest apiVersionRequest) {
        AbstractResponse apiResponse = ApiVersionsResponse.defaultApiVersionsResponse();
        ctx.writeAndFlush(responseToByteBuf(apiResponse, apiVersionRequest));
        return;
    }


    protected void handleError(String error) {
        throw new NotImplementedException("handleError");
    }

    protected void handleTopicMetadataRequest(KafkaHeaderAndRequest metadataHar) {
        checkArgument(metadataHar.getRequest() instanceof MetadataRequest);

        MetadataRequest metadataRequest = (MetadataRequest) metadataHar.getRequest();

        // Command response for all topics
        List<TopicMetadata> allTopicMetadata = Collections.synchronizedList(Lists.newArrayList());
        List<Node> allNodes = Collections.synchronizedList(Lists.newArrayList());

        List<String> topics = metadataRequest.topics();
        // topics in format : persistent://%s/%s/abc-partition-x, will be grouped by as:
        //      Entry<abc, List[TopicName]>
        CompletableFuture<Map<String, List<TopicName>>> pulsarTopicsFuture = new CompletableFuture<>();

        // 1. get list of pulsarTopics
        if (topics == null) {
            executor.execute(() -> {
                try {
                    Map<String, List<TopicName>> pulsarTopics =
                        kafkaService.getNamespaceService()
                            .getListOfPersistentTopics(kafkaNamespace)
                            .stream()
                            .map(topicString -> TopicName.get(topicString))
                            .collect(Collectors
                                .groupingBy(topicName -> getLocalNameWithoutPartition(topicName), Collectors.toList()));

                    if (log.isDebugEnabled()) {
                        log.debug("Get all topics in TopicMetadata request, will get {} topics", pulsarTopics.size());
                    }

                    pulsarTopicsFuture.complete(pulsarTopics);
                } catch (Exception e) {
                    // error when getListOfPersistentTopics
                    log.error("Failed to get all topics list", e);
                    pulsarTopicsFuture.completeExceptionally(e);
                }
            });
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
                                        Errors.UNKNOWN_SERVER_ERROR,
                                        topic,
                                        false,
                                        Collections.emptyList()));
                                log.warn("[{}] Failed to get partitioned topic metadata: {}",
                                    topicName, throwable.getMessage());
                            } else {
                                List<TopicName> topicNames;
                                if (partitionedTopicMetadata.partitions > 1) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Topic {} has {} partitions",
                                            topic, partitionedTopicMetadata.partitions);
                                    }
                                    topicNames = IntStream
                                        .range(0, partitionedTopicMetadata.partitions)
                                        .mapToObj(i ->
                                            TopicName.get(topicName.toString() + PARTITIONED_TOPIC_SUFFIX + i))
                                        .collect(Collectors.toList());

                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Topic {} has 1 partitions", topic);
                                    }
                                    topicNames = Lists.newArrayList(topicName);
                                }
                                pulsarTopics.put(topic, topicNames);
                            }

                            // whether handled all topics get partitions
                            int completedTopics = topicsCompleted.incrementAndGet();
                            if (completedTopics == topicsNumber) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Completed get {} topic's partitions", topicsNumber);
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
                log.warn("Exception fetching metadata, will return null Response", e);
                MetadataResponse finalResponse =
                    new MetadataResponse(
                        Collections.emptyList(),
                        clusterName,
                        MetadataResponse.NO_CONTROLLER_ID,
                        Collections.emptyList());
                ctx.writeAndFlush(responseToByteBuf(finalResponse, metadataHar));
                return;
            }

            final int topicsNumber = pulsarTopics.size();

            pulsarTopics.forEach((topic, list) -> {
                final int partitionsNumber = list.size();
                AtomicInteger partitionsCompleted = new AtomicInteger(0);
                List<PartitionMetadata> partitionMetadatas = Collections
                    .synchronizedList(Lists.newArrayListWithExpectedSize(partitionsNumber));

                list.forEach(topicName ->
                    findBroker(kafkaService, topicName)
                        .whenComplete(((partitionMetadata, throwable) -> {
                            if (throwable != null) {
                                log.warn("Exception while find Broker metadata", throwable);
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
                                log.debug("FindBroker for {} partitions of topic {}, total partitions: {}",
                                    finishedPartitions, topic, partitionsNumber);
                            }
                            if (finishedPartitions == partitionsNumber) {
                                // new TopicMetadata for this topic
                                allTopicMetadata.add(
                                    new TopicMetadata(Errors.NONE, topic, false, partitionMetadatas));

                                // whether completed all the topics requests.
                                int finishedTopics = topicsCompleted.incrementAndGet();
                                if (log.isDebugEnabled()) {
                                    log.debug("Completed findBroker for all partitions of topic {}, partitions: {}; "
                                            + "Finished Topics: {}, total topics: {}",
                                        topic, partitionsNumber, finishedTopics, topicsNumber);
                                }
                                if (finishedTopics == topicsNumber) {
                                    // TODO: confirm right value for controller_id
                                    MetadataResponse finalResponse =
                                        new MetadataResponse(
                                            allNodes,
                                            clusterName,
                                            0,
                                            allTopicMetadata);
                                    ctx.writeAndFlush(responseToByteBuf(finalResponse, metadataHar));
                                }
                            }
                        })));
            });
        });
    }

    protected void handleProduceRequest(KafkaHeaderAndRequest produceHar) {
        checkArgument(produceHar.getRequest() instanceof ProduceRequest);
        ProduceRequest produceRequest = (ProduceRequest) produceHar.getRequest();

        if (produceRequest.transactionalId() != null) {
            log.warn("[{}] Transactions not supported", ctx.channel());

            ctx.writeAndFlush(responseToByteBuf(
                failedResponse(produceHar, new UnsupportedOperationException("No transaction support")),
                produceHar));
            return;
        }

        // Ignore request.acks() and request.timeout(), which related to kafka replication in this broker.

        Map<TopicPartition, CompletableFuture<PartitionResponse>> responsesFutures = new HashMap<>();

        final int responsesSize = produceRequest.partitionRecordsOrFail().size();

        for (Map.Entry<TopicPartition, ? extends Records> entry : produceRequest.partitionRecordsOrFail().entrySet()) {
            TopicPartition topicPartition = entry.getKey();

            CompletableFuture<PartitionResponse> partitionResponse = new CompletableFuture<>();
            responsesFutures.put(topicPartition, partitionResponse);

            if (log.isDebugEnabled()) {
                log.debug("[{}] Produce messages for topic {} partition {}, request size: {} ",
                    ctx.channel(), topicPartition.topic(), topicPartition.partition(), responsesSize);
            }

            TopicName topicName = pulsarTopicName(topicPartition.topic(), topicPartition.partition());

            kafkaService.getBrokerService().getTopic(topicName.toString(), true)
                .whenComplete((topicOpt, exception) -> {
                    if (exception != null) {
                        log.error("[{}] Failed to getOrCreateTopic {}. exception:",
                            ctx.channel(), topicName, exception);
                        partitionResponse.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
                    } else {
                        if (topicOpt.isPresent()) {
                            publishMessages(entry.getValue(), topicOpt.get(), partitionResponse);
                        } else {
                            log.error("[{}] getOrCreateTopic get empty topic for name {}", ctx.channel(), topicName);
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
                    log.debug("[{}] Complete handle produce request {}.",
                        ctx.channel(), produceHar.toString());
                }
                ctx.writeAndFlush(responseToByteBuf(new ProduceResponse(responses), produceHar));
            });
        return;
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
                log.debug("[{}] publishMessages for topic partition: {} failed when write.",
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
                log.error("Failed write entry: {}, entryId: {}, sequenceId: {}. and triggered send callback.",
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
            msgMetadataBuilder.setProducerName(producerName);
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


    protected void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator) {
        throw new NotImplementedException("handleFindCoordinatorRequest");
    }

    protected void handleListOffsetRequest(KafkaHeaderAndRequest listOffset) {
        throw new NotImplementedException("handleListOffsetRequest");
    }

    protected void handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch) {
        throw new NotImplementedException("handleOffsetFetchRequest");
    }

    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit) {
        throw new NotImplementedException("handleOffsetCommitRequest");
    }

    protected void handleFetchRequest(KafkaHeaderAndRequest fetch) {
        throw new NotImplementedException("handleFetchRequest");
    }

    protected void handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup) {
        throw new NotImplementedException("handleFetchRequest");
    }

    protected void handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup) {
        throw new NotImplementedException("handleSyncGroupRequest");
    }

    protected void handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat) {
        throw new NotImplementedException("handleHeartbeatRequest");
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
                            log.debug("Find broker: {} for topicName: {}", uri, topic);
                        }

                        Node node = newNode(new InetSocketAddress(uri.getHost(), uri.getPort()));
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
        return new Node(0, address.getHostString(), address.getPort());
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
