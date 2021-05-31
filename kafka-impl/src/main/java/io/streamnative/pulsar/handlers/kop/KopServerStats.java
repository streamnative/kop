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

/**
 * Kop server stats for prometheus metrics.
 */
public interface KopServerStats {
    String CATEGORY_SERVER = "server";

    String SERVER_SCOPE = "kop_server";

    String REQUEST_SCOPE = "request";
    String TOPIC_SCOPE = "topic";
    String PARTITION_SCOPE = "partition";
    String GROUP_SCOPE = "group";

    /**
     * Request stats.
     */
    String REQUEST_QUEUE_SIZE = "REQUEST_QUEUE_SIZE";
    String REQUEST_QUEUED_LATENCY = "REQUEST_QUEUED_LATENCY";
    String REQUEST_PARSE = "REQUEST_PARSE";
    String REQUEST_LATENCY = "REQUEST_LATENCY";

    /**
     * Response stats.
     */
    String RESPONSE_QUEUE_SIZE = "RESPONSE_QUEUE_SIZE";
    String RESPONSE_BLOCKED_TIMES = "RESPONSE_BLOCKED_TIMES";
    String RESPONSE_BLOCKED_LATENCY = "RESPONSE_BLOCKED_LATENCY";

    /**
     * PRODUCE STATS.
     */
    String PRODUCE_ENCODE = "PRODUCE_ENCODE";
    String MESSAGE_PUBLISH = "MESSAGE_PUBLISH";
    String MESSAGE_QUEUED_LATENCY = "MESSAGE_QUEUED_LATENCY";

    /**
     * FETCH stats.
     *
     * <p>
     * Elapsed time estimation:
     * 1) HANDLE_FETCH_REQUEST = PREPARE_METADATA + TOTAL_MESSAGE_READ + FETCH_DECODE + Overhead
     * 2) TOTAL_MESSAGE_READ = read-recursion-times * topic-partitions * MESSAGE_READ + Overhead
     * </p>
     */
    String PREPARE_METADATA = "PREPARE_METADATA";
    String MESSAGE_READ = "MESSAGE_READ";
    String FETCH_DECODE = "FETCH_DECODE";

    /**
     * Consumer stats.
     */
    String BYTES_OUT = "BYTES_OUT";
    String MESSAGE_OUT = "MESSAGE_OUT";
    String ENTRIES_OUT = "ENTRIES_OUT";
}
