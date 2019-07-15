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
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class KafkaRequestHandler extends KafkaCommandDecoder {

    private final KafkaService kafkaService;
    private final String clusterName;
    private final NamespaceName kafkaNamespace;
    private final ExecutorService executor;
    private final PulsarAdmin admin;

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

        MetadataRequest metadataRequest = (MetadataRequest)metadataHar.getRequest();

        // Command response for all topics
        List<TopicMetadata> allTopicMetadata = new ArrayList<>();
        List<Node> allNodes = new ArrayList<>();

        List<String> topics = metadataRequest.topics();
        // topics in format : persistent://%s/%s/abc-partition-x, will be grouped by as:
        //      Entry<abc, List[TopicName]>
        CompletableFuture<Map<String, List<TopicName>>> pulsarTopicsFuture = new CompletableFuture<>();

        // 1. get list of pulsarTopics
        if (topics == null) {
            executor.execute(() -> {
                try {
                    Map<String, List<TopicName>>  pulsarTopics =
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
                    TopicName topicName = TopicName.get(TopicDomain.persistent.value(), kafkaNamespace, topic);
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
                                    new TopicMetadata(Errors.INVALID_PARTITIONS, topic, false, Collections.EMPTY_LIST));
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
                    new MetadataResponse(Collections.EMPTY_LIST, clusterName, MetadataResponse.NO_CONTROLLER_ID, Collections.EMPTY_LIST);
                ctx.writeAndFlush(responseToByteBuf(finalResponse, metadataHar));
                return;
            }

            final int topicsNumber = pulsarTopics.size();

            pulsarTopics.forEach((topic, list) -> {
                final int partitionsNumber = list.size();
                AtomicInteger partitionsCompleted = new AtomicInteger(0);
                List<PartitionMetadata> partitionMetadatas = Lists.newArrayListWithExpectedSize(partitionsNumber);

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
                            if (finishedPartitions == partitionsNumber) {
                                // new TopicMetadata for this topic
                                allTopicMetadata.add(
                                    new TopicMetadata(Errors.NONE, topic, false, partitionMetadatas));

                                // whether completed all the topics requests.
                                int finishedTopics = topicsCompleted.incrementAndGet();
                                if (finishedTopics == topicsNumber) {
                                    // TODO: confirm right value for controller_id
                                    MetadataResponse finalResponse =
                                        new MetadataResponse(allNodes, clusterName, MetadataResponse.NO_CONTROLLER_ID, allTopicMetadata);
                                    ctx.writeAndFlush(responseToByteBuf(finalResponse, metadataHar));
                                }
                            }
                        })));
            });
        });
    }

    protected void handleProduceRequest(KafkaHeaderAndRequest produce) {
        throw new NotImplementedException("handleProduceRequest");
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

    // TODO: use binary proto to find Broker to improve performance.
    private CompletableFuture<PartitionMetadata> findBroker(KafkaService kafkaService, TopicName topic) {
        if (log.isDebugEnabled()) {
            log.debug("Handle Lookup for {}", topic);
        }

        final CompletableFuture<PartitionMetadata> resultFuture = new CompletableFuture<>();

        executor.execute(() -> {
            try {
                String broker = kafkaService.getAdminClient().lookups().lookupTopic(topic.toString());
                URI uri = new URI(broker);
                Node node = newNode(new InetSocketAddress(uri.getHost(), uri.getPort()));

                resultFuture.complete(newPartitionMetadata(topic, node));
            } catch (Exception e) {
                log.error("Caught error while find Broker for topic:{} ", topic, e);
                resultFuture.completeExceptionally(e);
            }
        });
        return resultFuture;
    }


    private static Errors pulsarToKafkaException(AbstractRequest request, Throwable exception) {
        if (log.isDebugEnabled()) {
            log.debug("Converting error for request {}", request, exception);
        }
        if (exception instanceof PulsarAdminException.NotFoundException) {
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        } else if (exception.getCause() != null) {
            return pulsarToKafkaException(request, exception.getCause());
        } else {
            return Errors.UNKNOWN_SERVER_ERROR;
        }
    }

    // TODO: handle Kafka Node.id
    private static Node newNode(InetSocketAddress address) {
        if (log.isDebugEnabled()) {
            log.debug("Return Broker Node of {}", address);
        }
        return new Node(0, address.getHostString(), address.getPort());
    }

    private static PartitionMetadata newPartitionMetadata(TopicName topicName, Node node) {
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
            Collections.EMPTY_LIST     // offline replicas
        );
    }

    private static PartitionMetadata newFailedPartitionMetadata(TopicName topicName) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        log.warn("Failed find Broker metadata, create PartitionMetadata with INVALID_PARTITIONS");

        return new PartitionMetadata(
            Errors.INVALID_PARTITIONS,
            kafkaPartitionIndex,
            Node.noNode(),                      // leader
            Lists.newArrayList(Node.noNode()),  // replicas
            Lists.newArrayList(Node.noNode()),  // isr
            Collections.EMPTY_LIST              // offline replicas
        );
    }

    private static String getLocalNameWithoutPartition(TopicName topicName) {
        String localName = topicName.getLocalName();
        if (localName.contains(PARTITIONED_TOPIC_SUFFIX)) {
            return localName.substring(0, localName.lastIndexOf(PARTITIONED_TOPIC_SUFFIX));
        } else {
            return localName;
        }
    }
}
