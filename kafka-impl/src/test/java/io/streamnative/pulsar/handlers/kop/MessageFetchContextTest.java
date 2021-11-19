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

import static org.testng.Assert.assertFalse;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ResponseCallbackWrapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class MessageFetchContextTest {

    private static final Map<TopicPartition, FetchRequest.PartitionData> fetchData = Maps.newHashMap();

    private static CompletableFuture<AbstractResponse> resultFuture = null;

    FetchRequest fetchRequest = FetchRequest.Builder
            .forConsumer(0, 0, fetchData).build();

    private static final String topicName = "test-fetch";
    private static final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private static final TopicPartition tp2 = new TopicPartition(topicName, 2);

    private static volatile MessageFetchContext messageFetchContext = null;

    @BeforeMethod
    protected void setup() throws Exception {
        fetchData.put(tp1, null);
        fetchData.put(tp2, null);
        resultFuture = new CompletableFuture<>();
        messageFetchContext = MessageFetchContext.getForTest(fetchRequest, "public/default", resultFuture);
    }

    private void startThreads() throws Exception {
        Thread run1 = new Thread(() -> {
            // For the synchronous method, we can check it only once,
            // because if there is still has problem, it will eventually become a flaky test,
            // If it does not become a flaky test, then we can keep this
            messageFetchContext.addErrorPartitionResponseForTest(tp1, Errors.NONE);
        });

        Thread run2 = new Thread(() -> {
            // As comment described in run1, we can check it only once.
            messageFetchContext.addErrorPartitionResponseForTest(tp2, Errors.NONE);
        });

        run1.start();
        run2.start();
        run1.join();
        run2.join();
    }

    private void startAndGetResult(AtomicReference<Set<Errors>> errorsSet)
            throws Exception {

        BiConsumer<AbstractResponse, Throwable> action = (response, exception) -> {
            ResponseCallbackWrapper responseCallbackWrapper = (ResponseCallbackWrapper) response;
            Set<Errors> currentErrors = errorsSet.get();
            currentErrors.addAll(responseCallbackWrapper.errorCounts().keySet());
            errorsSet.set(currentErrors);
        };

        startThreads();
        resultFuture.whenComplete(action);
    }

    // Run the actually modified code logic in MessageFetchContext
    // to avoid changing the MessageFetchContext in the future
    // and failing to catch possible errors.
    // We need to ensure that resultFuture.complete has no REQUEST_TIMED_OUT error.
    @Test
    public void testHandleFetchSafe() throws Exception {
        AtomicReference<Set<Errors>> errorsSet = new AtomicReference<>(new HashSet<>());
        startAndGetResult(errorsSet);
        assertFalse(errorsSet.get().contains(Errors.REQUEST_TIMED_OUT));
    }

}
