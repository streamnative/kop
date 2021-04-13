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

    /**
     * Request stats.
     */
    String REQUEST_QUEUE_SIZE = "REQUEST_QUEUE_SIZE";
    String REQUEST_QUEUED_LATENCY = "REQUEST_QUEUED_LATENCY";
    String REQUEST_PARSE = "REQUEST_PARSE";

    /**
     * PRODUCE STATS.
     */
    String HANDLE_PRODUCE_REQUEST = "HANDLE_PRODUCE_REQUEST";
    String PRODUCE_ENCODE = "PRODUCE_ENCODE";
    String MESSAGE_PUBLISH = "MESSAGE_PUBLISH";
    String MESSAGE_QUEUED_LATENCY = "MESSAGE_QUEUED_LATENCY";

}
