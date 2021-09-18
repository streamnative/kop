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
import static io.streamnative.pulsar.handlers.kop.KopResponseManager.responseToByteBuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.ShutdownableThread;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ResponseCallbackWrapper;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;

@Slf4j
@Getter
public class KopEventManager {
    private static final String REGEX = "^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    private final String kopEventThreadName;
    private final ReentrantLock putLock = new ReentrantLock();
    private final LinkedBlockingQueue<KopEvent> queue;
    private final KopEventThread thread;
    private final GroupCoordinator coordinator;
    private final AdminManager adminManager;
    private DeletionTopicsHandler deletionTopicsHandler;
    private BrokersChangeHandler brokersChangeHandler;
    private final MetadataStore metadataStore;

    public KopEventManager(GroupCoordinator coordinator,
                           AdminManager adminManager,
                           MetadataStore metadataStore,
                           String kopEventThreadName) {
        this.coordinator = coordinator;
        this.adminManager = adminManager;
        this.metadataStore = metadataStore;
        this.kopEventThreadName = kopEventThreadName;
        this.queue = new LinkedBlockingQueue<>();
        this.thread = new KopEventThread(kopEventThreadName, this);
    }

    public void registerAndStart() {
        this.deletionTopicsHandler = new DeletionTopicsHandler(this);
        this.brokersChangeHandler = new BrokersChangeHandler(this);
        registerChildChangeHandler();
        start();
    }

    public void start() {
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
        putLock.lock();
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Error put event {} to coordinator event queue", event, e);
        } finally {
            putLock.unlock();
        }
    }

    public void clearAndPut(KopEvent event) {
        putLock.lock();
        try {
            queue.clear();
            put(event);
        } finally {
            putLock.unlock();
        }
    }

    static class KopEventThread extends ShutdownableThread {
        private final KopEventManager kopEventManager;

        public KopEventThread(String name,
                              KopEventManager kopEventManager) {
            super(name);
            this.kopEventManager = kopEventManager;
        }

        @Override
        protected void doWork() {
            KopEvent event = null;
            try {
                event = kopEventManager.getQueue().take();
                if (event instanceof KopRequestEvent) {
                    KopRequestEvent requestEvent = (KopRequestEvent) event;
                    requestEvent.process();
                    KafkaCommandDecoder.ResponseAndRequest responseAndRequest = requestEvent.getResponseAndRequest();

                    while (true) {
                        if (requestEvent.isCompleted()) {
                            break;
                        }

                        final long nanoSecondsSinceCreated = responseAndRequest.nanoSecondsSinceCreated();
                        final boolean expired =
                                (nanoSecondsSinceCreated > TimeUnit.MILLISECONDS.toNanos(
                                        requestEvent.getRequestTimeoutMs()));

                        if (expired) {

                            if (log.isDebugEnabled()) {
                                log.debug("Handle {} timeout.", responseAndRequest.getRequest());
                            }

                            // Immediately trigger request processing failure
                            responseAndRequest.getResponseFuture().completeExceptionally(
                                    new ApiException("request is expired from server side"));
                            break;
                        }
                    }
                } else {
                    event.process();
                }
            } catch (InterruptedException e) {
                log.error("Error processing event {}", event, e);
            }
        }

    }

    private void registerChildChangeHandler() {
        metadataStore.registerListener(this::handleChildChangePathNotification);

        // Really register ChildChange notification.
        metadataStore.getChildren(getDeleteTopicsPath());
        // init local kop brokers cache
        getBrokers(metadataStore.getChildren(getBrokersChangePath()).join());
    }

    private void handleChildChangePathNotification(Notification notification) {
        if (notification.getPath().equals(LoadManager.LOADBALANCE_BROKERS_ROOT)) {
            this.brokersChangeHandler.handleChildChange();
        } else if (notification.getPath().equals(getDeleteTopicsPath())) {
            this.deletionTopicsHandler.handleChildChange();
        }
    }

    private void getBrokers(List<String> pulsarBrokers) {
        final Set<Node> kopBrokers = Sets.newConcurrentHashSet();
        final AtomicInteger pendingBrokers = new AtomicInteger(pulsarBrokers.size());

        pulsarBrokers.forEach(broker -> {
            metadataStore.get(getBrokersChangePath() + "/" + broker).whenComplete(
                    (brokerData, e) -> {
                        if (e != null) {
                            log.error("Get broker {} path data failed which have an error", broker, e);
                            return;
                        }

                        if (brokerData.isPresent()) {
                            JsonObject jsonObject = parseJsonObject(
                                    new String(brokerData.get().getValue(), StandardCharsets.UTF_8));
                            JsonObject protocols = jsonObject.getAsJsonObject("protocols");
                            JsonElement element = protocols.get("kafka");

                            if (element != null) {
                                String kopBrokerStr = element.getAsString();
                                Node kopNode = getNode(kopBrokerStr);
                                kopBrokers.add(kopNode);
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
                            Collection<? extends Node> oldKopBrokers = adminManager.getBrokers();
                            adminManager.setBrokers(kopBrokers);
                            log.info("Refresh kop brokers new cache {}, old brokers cache {}",
                                    adminManager.getBrokers(), oldKopBrokers);
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
    public static Node getNode(String kopBrokerStr) {
        final String errorMessage = "kopBrokerStr " + kopBrokerStr + " is invalid";
        final Matcher matcher = PATTERN.matcher(kopBrokerStr);
        checkState(matcher.find(), errorMessage);
        checkState(matcher.groupCount() == 3, errorMessage);
        String host = matcher.group(2);
        String port = matcher.group(3);

        return new Node(
                Murmur3_32Hash.getInstance().makeHash((host + port).getBytes(StandardCharsets.UTF_8)),
                host,
                Integer.parseInt(port));
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
                List<String> topicsDeletions = metadataStore.getChildren(getDeleteTopicsPath()).get();

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

                    deletedTopics.forEach(deletedTopic -> {
                        metadataStore.delete(
                                getDeleteTopicsPath() + "/" + deletedTopic, Optional.of((long) -1));
                    });
                }

                log.info("GroupMetadata delete topics {}, no matching topics {}",
                        deletedTopics, Sets.difference(topicsFullNameDeletionsSets, deletedTopics));

            } catch (ExecutionException | InterruptedException e) {
                log.error("DeleteTopicsEvent process have an error", e);
            }
        }
    }

    class BrokersChangeEvent implements KopEvent {
        @Override
        public void process() {
            metadataStore.getChildren(getBrokersChangePath()).whenComplete(
                    (brokers, e) -> {
                        if (e != null) {
                            log.error("BrokersChangeEvent process have an error", e);
                            return;
                        }
                        getBrokers(brokers);
                    });
        }
    }

    static class KopResponseEvent implements KopEvent {
        private final Channel channel;
        private final KafkaCommandDecoder.ResponseAndRequest responseAndRequest;
        private final RequestStats requestStats;
        private final int requestTimeoutMs;

        public KopResponseEvent(Channel channel,
                                KafkaCommandDecoder.ResponseAndRequest responseAndRequest,
                                RequestStats requestStats,
                                int requestTimeoutMs) {
            this.channel = channel;
            this.responseAndRequest = responseAndRequest;
            this.requestStats = requestStats;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        @Override
        public void process() {
            if (channel.isActive()) {
                if (responseAndRequest == null) {
                    return;
                }

                final CompletableFuture<AbstractResponse> responseFuture = responseAndRequest.getResponseFuture();
                final long nanoSecondsSinceCreated = responseAndRequest.nanoSecondsSinceCreated();
                final boolean expired =
                        (nanoSecondsSinceCreated > TimeUnit.MILLISECONDS.toNanos(requestTimeoutMs));

                if (responseAndRequest.getFirstBlockedTimestamp() != 0) {
                    requestStats.getResponseBlockedLatency().registerSuccessfulEvent(
                            MathUtils.elapsedNanos(responseAndRequest.getFirstBlockedTimestamp()),
                            TimeUnit.NANOSECONDS);
                }

                final KafkaCommandDecoder.KafkaHeaderAndRequest request = responseAndRequest.getRequest();

                // responseFuture is completed exceptionally
                if (responseFuture.isCompletedExceptionally()) {
                    responseFuture.exceptionally(e -> {
                        log.error("[{}] request {} completed exceptionally", channel, request.getHeader(), e);
                        channel.writeAndFlush(request.createErrorResponse(e));

                        requestStats.getStatsLogger()
                                .scopeLabel(KopServerStats.REQUEST_SCOPE,
                                        responseAndRequest.getRequest().getHeader().apiKey().name)
                                .getOpStatsLogger(KopServerStats.REQUEST_QUEUED_LATENCY)
                                .registerFailedEvent(MathUtils.elapsedNanos(responseAndRequest.getCreatedTimestamp()),
                                        TimeUnit.NANOSECONDS);
                        return null;
                    }); // send exception to client?
                } else if (responseFuture.isDone()) {
                    // responseFuture is completed normally
                    responseFuture.thenAccept(response -> {
                        if (response == null) {
                            // It should not be null, just check it for safety
                            log.error("[{}] Unexpected null completed future for request {}",
                                    channel, request.getHeader());
                            channel.writeAndFlush(request.createErrorResponse(new ApiException("response is null")));
                            return;
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Write kafka cmd to client."
                                            + " request content: {}"
                                            + " responseAndRequest content: {}",
                                    request, response.toString(request.getRequest().version()));
                        }

                        final ByteBuf result = responseToByteBuf(response, request);
                        channel.writeAndFlush(result).addListener(future -> {
                            if (response instanceof ResponseCallbackWrapper) {
                                ((ResponseCallbackWrapper) response).responseComplete();
                            }
                            if (!future.isSuccess()) {
                                log.error("[{}] Failed to write {}", channel, request.getHeader(), future.cause());
                            }
                        });
                    });
                } else if (expired) {
                    // responseFuture is expired
                    log.error("[{}] request {} is not completed for {} ns (> {} ms)",
                            channel, request.getHeader(), nanoSecondsSinceCreated, requestTimeoutMs);
                    responseFuture.cancel(true);
                    channel.writeAndFlush(
                            request.createErrorResponse(new ApiException("request is expired from server side")));

                    requestStats.getStatsLogger()
                            .scopeLabel(KopServerStats.REQUEST_SCOPE,
                                    responseAndRequest.getRequest().getHeader().apiKey().name)
                            .getOpStatsLogger(KopServerStats.REQUEST_QUEUED_LATENCY)
                            .registerFailedEvent(MathUtils.elapsedNanos(responseAndRequest.getCreatedTimestamp()),
                                    TimeUnit.NANOSECONDS);
                }
            }
        }
    }

    @Getter
    static class KopRequestEvent implements KopEvent {
        private final KafkaCommandDecoder.ResponseAndRequest responseAndRequest;
        private final KafkaCommandDecoder decoder;
        private final long requestTimeoutMs;

        public KopRequestEvent(KafkaCommandDecoder.ResponseAndRequest responseAndRequest,
                               KafkaCommandDecoder decoder,
                               long requestTimeoutMs) {
            this.responseAndRequest = responseAndRequest;
            this.decoder = decoder;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        private boolean isCompleted() {
            return responseAndRequest.getResponseFuture().isDone()
                    || responseAndRequest.getResponseFuture().isCompletedExceptionally()
                    || responseAndRequest.getResponseFuture().isCancelled();
        }

        @Override
        public void process() {
            decoder.handleKafkaRequest(responseAndRequest.getRequest(), responseAndRequest.getResponseFuture());
        }
    }

    public DeleteTopicsEvent getDeleteTopicEvent() {
        return new DeleteTopicsEvent();
    }

    public BrokersChangeEvent getBrokersChangeEvent() {
        return new BrokersChangeEvent();
    }

    public KopResponseEvent getKopResponseEvent(Channel channel,
                                                KafkaCommandDecoder.ResponseAndRequest responseAndRequest,
                                                RequestStats requestStats,
                                                int requestTimeoutMs) {
        return new KopResponseEvent(channel,
                responseAndRequest,
                requestStats,
                requestTimeoutMs);
    }

    public KopRequestEvent getKopRequestEvent(KafkaCommandDecoder.ResponseAndRequest responseAndRequest,
                                              KafkaCommandDecoder decoder,
                                              long requestTimeoutMs) {
        return new KopRequestEvent(responseAndRequest,
                decoder,
                requestTimeoutMs);
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
