package io.streamnative.pulsar.handlers.kop;

import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ResponseCallbackWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.collections.Maps;

public class MessageFetchContextTest {

    private static final Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>>
            responseData = new ConcurrentHashMap();

    private static final Map<TopicPartition, FetchRequest.PartitionData> fetchData = Maps.newHashMap();

    private static CompletableFuture<AbstractResponse> resultFuture = null;

    FetchRequest fetchRequest = FetchRequest.Builder
            .forConsumer(0, 0, fetchData).build();

    private static final ConcurrentLinkedQueue<DecodeResult> decodeResults = new ConcurrentLinkedQueue<>();

    private static final TopicPartition tp1 = new TopicPartition("test-fetch", 1);
    private static final TopicPartition tp2 = new TopicPartition("test-fetch", 2);

    private static final ExecutorService threadPool = Executors.newFixedThreadPool(2);

    // Competitive problems need a certain chance to appear.
    // Try a few more times to have a higher chance of reproducing the problem.
    private static final int totalAttempts = 200;

    private static final AtomicInteger currentAttempts = new AtomicInteger(0);

    @Before
    public void init() {
        fetchData.put(tp1, null);
        fetchData.put(tp2, null);
        resultFuture = new CompletableFuture<>();
    }

    private void startThreads(Boolean isSafe) throws ExecutionException, InterruptedException {
        Runnable run1 = () -> {
            for (int i = 0; i < 100; i++) {
                addErrorPartitionResponse(tp1, Errors.NONE, isSafe);
            }
        };

        Runnable run2 = () -> {
            for (int i = 0; i < 100; i++) {
                addErrorPartitionResponse(tp2, Errors.NONE, isSafe);
            }
        };
        Future<?> future1 = threadPool.submit(run1);
        Future<?> future2 = threadPool.submit(run2);
        future1.get();
        future2.get();
    }

    private void startAndGetResult(Boolean isSafe, AtomicReference<Set<Errors>> errorsSet)
            throws ExecutionException, InterruptedException {

        BiConsumer<AbstractResponse, Throwable> action = (response, exception) -> {
            ResponseCallbackWrapper responseCallbackWrapper = (ResponseCallbackWrapper) response;
            Set<Errors> currentErrors = errorsSet.get();
            currentErrors.addAll(responseCallbackWrapper.errorCounts().keySet());
            errorsSet.set(currentErrors);
        };

        while (currentAttempts.getAndIncrement() <= totalAttempts) {
            startThreads(isSafe);
            resultFuture.whenComplete(action);
            resultFuture = new CompletableFuture<>();
            responseData.clear();
        }
    }

    @Test
    public void testHandleFetchUnSafe() throws ExecutionException, InterruptedException {
        AtomicReference<Set<Errors>> errorsSet = new AtomicReference<>(new HashSet<>());
        startAndGetResult(false, errorsSet);
        assertTrue(errorsSet.get().contains(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    public void testHandleFetchSafe() throws ExecutionException, InterruptedException {
        AtomicReference<Set<Errors>> errorsSet = new AtomicReference<>(new HashSet<>());
        startAndGetResult(true, errorsSet);
        assertFalse(errorsSet.get().contains(Errors.REQUEST_TIMED_OUT));
    }

    private void addErrorPartitionResponse(TopicPartition topicPartition, Errors errors, Boolean isSafe) {
        responseData.put(topicPartition, new FetchResponse.PartitionData<>(
                errors,
                FetchResponse.INVALID_HIGHWATERMARK,
                FetchResponse.INVALID_LAST_STABLE_OFFSET,
                FetchResponse.INVALID_LOG_START_OFFSET,
                null,
                MemoryRecords.EMPTY));
        if (isSafe) {
            tryCompleteThreadSafe();
        } else {
            tryComplete();
        }
    }

    private void tryComplete() {
        if (responseData.size() >= fetchRequest.fetchData().size()) {
            complete();
        }
    }

    private synchronized void tryCompleteThreadSafe() {
        if (responseData.size() >= fetchRequest.fetchData().size()) {
            complete();
        }
    }

    private void complete() {
        if (resultFuture == null) {
            // the context has been recycled
            return;
        }
        if (resultFuture.isCancelled()) {
            // The request was cancelled by KafkaCommandDecoder when channel is closed or this request is expired,
            // so the Netty buffers should be released.
            decodeResults.forEach(DecodeResult::release);
            return;
        }
        if (resultFuture.isDone()) {
            // It may be triggered again in DelayedProduceAndFetch
            return;
        }

        // Keep the order of TopicPartition
        final LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>>
                orderedResponseData = new LinkedHashMap<>();
        // add the topicPartition with timeout error if it's not existed in responseData
        fetchRequest.fetchData().keySet().forEach(topicPartition -> {
            final FetchResponse.PartitionData<MemoryRecords> partitionData = responseData.remove(topicPartition);
            if (partitionData != null) {
                orderedResponseData.put(topicPartition, partitionData);
            } else {
                orderedResponseData.put(topicPartition, new FetchResponse.PartitionData<>(
                        Errors.REQUEST_TIMED_OUT,
                        FetchResponse.INVALID_HIGHWATERMARK,
                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                        FetchResponse.INVALID_LOG_START_OFFSET,
                        null,
                        MemoryRecords.EMPTY));
            }
        });

        // Create another reference to this.decodeResults so the lambda expression will capture this local reference
        // because this.decodeResults will be reset to null after resultFuture is completed.
        final ConcurrentLinkedQueue<DecodeResult> decodeResults = MessageFetchContextTest.decodeResults;
        resultFuture.complete(
                new ResponseCallbackWrapper(
                        new FetchResponse<>(
                                Errors.NONE,
                                orderedResponseData,
                                ((Integer) THROTTLE_TIME_MS.defaultValue),
                                fetchRequest.metadata().sessionId()),
                        () -> {
                            // release the batched ByteBuf if necessary
                            decodeResults.forEach(DecodeResult::release);
                        }));
    }

    @After
    public void close() {
        threadPool.shutdown();
    }
}
