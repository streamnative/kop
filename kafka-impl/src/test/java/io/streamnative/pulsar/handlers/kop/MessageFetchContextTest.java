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

import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class MessageFetchContextTest {

    private static final Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>>
            responseData = new ConcurrentHashMap();

    private static final Map<TopicPartition, FetchRequest.PartitionData> fetchData = Maps.newHashMap();

    private static CompletableFuture<AbstractResponse> resultFuture = null;

    FetchRequest fetchRequest = FetchRequest.Builder
            .forConsumer(0, 0, fetchData).build();

    private static final ConcurrentLinkedQueue<DecodeResult> decodeResults = new ConcurrentLinkedQueue<>();

    private static final String topicName = "test-fetch";
    private static final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private static final TopicPartition tp2 = new TopicPartition(topicName, 2);

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
            2, 2, 1000, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(2), Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.DiscardPolicy());


    // Competitive problems need a certain chance to appear.
    // Try a few more times to have a higher chance of reproducing the problem.
    private static final int totalAttempts = 200;

    private static final AtomicInteger currentAttempts = new AtomicInteger(0);

    private static volatile MessageFetchContext messageFetchContext = null;

    @BeforeMethod
    protected void setup() throws Exception {
        fetchData.put(tp1, null);
        fetchData.put(tp2, null);
        resultFuture = new CompletableFuture<>();
        messageFetchContext = MessageFetchContext.getForTest(fetchRequest, resultFuture);
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        threadPoolExecutor.shutdown();
    }

    private void startThreads(Boolean isSafe) throws Exception {
        Runnable run1 = () -> {
            if (isSafe) {
                // For the synchronous method, we can check it only once,
                // because if there is still has problem, it will eventually become a flaky test,
                // If it does not become a flaky test, then we can keep this
                messageFetchContext.addErrorPartitionResponseForTest(tp1, Errors.NONE);
            } else {
                for (int i = 0; i < 100; i++) {
                    addErrorPartitionResponse(tp1, Errors.NONE);
                }
            }
        };

        Runnable run2 = () -> {
            if (isSafe) {
                // As comment described in run1, we can check it only once.
                messageFetchContext.addErrorPartitionResponseForTest(tp2, Errors.NONE);
            } else {
                for (int i = 0; i < 100; i++) {
                    addErrorPartitionResponse(tp2, Errors.NONE);
                }
            }
        };

        Future<?> future1 = threadPoolExecutor.submit(run1);
        Future<?> future2 = threadPoolExecutor.submit(run2);
        future1.get();
        future2.get();
    }

    private void startAndGetResult(Boolean isSafe, AtomicReference<Set<Errors>> errorsSet, Boolean isBlock)
            throws Exception {

        BiConsumer<AbstractResponse, Throwable> action = (response, exception) -> {
            ResponseCallbackWrapper responseCallbackWrapper = (ResponseCallbackWrapper) response;
            Set<Errors> currentErrors = errorsSet.get();
            currentErrors.addAll(responseCallbackWrapper.errorCounts().keySet());
            errorsSet.set(currentErrors);
        };

        while (currentAttempts.getAndIncrement() <= totalAttempts || isBlock) {
            startThreads(isSafe);
            resultFuture.whenComplete(action);
            resultFuture = new CompletableFuture<>();
            responseData.clear();
            if ((isBlock && errorsSet.get().contains(Errors.REQUEST_TIMED_OUT)) || isSafe) {
                break;
            }
        }
    }

    // Since the code has been changed to a synchronized state,
    // competitive problem is reproduced by simulating the code before the modification
    @Test
    public void testHandleFetchUnSafe() throws Exception {
        AtomicReference<Set<Errors>> errorsSet = new AtomicReference<>(new HashSet<>());
        startAndGetResult(false, errorsSet, true);
        assertTrue(errorsSet.get().contains(Errors.REQUEST_TIMED_OUT));
    }

    // Run the actually modified code logic in MessageFetchContext
    // to avoid changing the MessageFetchContext in the future
    // and failing to catch possible errors.
    // We need to ensure that resultFuture.complete has no REQUEST_TIMED_OUT error.
    @Test
    public void testHandleFetchSafe() throws Exception {
        AtomicReference<Set<Errors>> errorsSet = new AtomicReference<>(new HashSet<>());
        startAndGetResult(true, errorsSet, false);
        assertFalse(errorsSet.get().contains(Errors.REQUEST_TIMED_OUT));
    }

    private void addErrorPartitionResponse(TopicPartition topicPartition, Errors errors) {
        responseData.put(topicPartition, new FetchResponse.PartitionData<>(
                errors,
                FetchResponse.INVALID_HIGHWATERMARK,
                FetchResponse.INVALID_LAST_STABLE_OFFSET,
                FetchResponse.INVALID_LOG_START_OFFSET,
                null,
                MemoryRecords.EMPTY));

        tryComplete();
    }

    private void tryComplete() {
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
}
