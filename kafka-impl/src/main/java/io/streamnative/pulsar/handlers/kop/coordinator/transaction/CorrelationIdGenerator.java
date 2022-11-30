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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

public class CorrelationIdGenerator {

    private static final int MAX_RESERVED_CORRELATION_ID = Integer.MAX_VALUE;
    private static final int MIN_RESERVED_CORRELATION_ID = MAX_RESERVED_CORRELATION_ID - 7;
    private int correlationId = 0;

    /**
     * See <a href="https://github.com/apache/kafka/pull/8471" /> for the reserved correlation id design.
     */
    public synchronized int next() {
        if (isReserved(correlationId)) {
            correlationId = MIN_RESERVED_CORRELATION_ID;
        }
        return correlationId++;
    }

    private static boolean isReserved(int correlationId) {
        return correlationId >= MIN_RESERVED_CORRELATION_ID;
    }
}
