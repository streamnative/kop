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

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Maps;
import io.streamnative.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.kop.KafkaCommandDecoder.ResponseAndRequest;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.testng.annotations.Test;

/**
 * Validate LogOffset.
 */
@Slf4j
public class LogOffsetTest extends KafkaApisTest {

    @Test(timeOut = 20000)
    public void testGetOffsetsForUnknownTopic() throws Exception {
        String topicName = "kopTestGetOffsetsForUnknownTopic";

        TopicPartition tp = new TopicPartition(topicName, 0);
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.LATEST_TIMESTAMP);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<ResponseAndRequest> responseFuture = kafkaRequestHandler
            .handleListOffsetRequest(request);

        ResponseAndRequest response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response.getResponse();
        assertEquals(response.getRequest().getHeader().apiKey(), ApiKeys.LIST_OFFSETS);
        assertEquals(listOffsetResponse.responseData().get(tp).error,
            Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }
}
