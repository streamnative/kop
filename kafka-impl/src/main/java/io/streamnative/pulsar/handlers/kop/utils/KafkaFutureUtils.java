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
package io.streamnative.pulsar.handlers.kop.utils;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.KafkaFuture;

/**
 * Utility class for working with KafkaFuture.
 */
public class KafkaFutureUtils {
    /**
     * Convert a KafkaFuture to a CompletableFuture.
     * @param kafkaFuture the KafkaFuture to be converted
     * @param <T> type of the value produced by the KafkaFuture
     * @return the corresponding CompletableFuture
     */
    public static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
        // https://gist.github.com/bmaggi/8e42a16a02f18d3bff9b0b742a75bfe7
        CompletableFuture<T> wrappingFuture = new CompletableFuture<>();
        kafkaFuture.whenComplete((value, throwable) -> {
            if (throwable != null) {
                wrappingFuture.completeExceptionally(throwable);
            } else {
                wrappingFuture.complete(value);
            }
        });
        return wrappingFuture;
    }
}
