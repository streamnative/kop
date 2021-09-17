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

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ResponseUtils;

@Getter
@Slf4j
public class KopResponseManager {
    private final KafkaServiceConfiguration kafkaConfig;
    private final List<KopEventManager> responseHandles;
    private final KopRequestManager requestManager;

    public KopResponseManager(KafkaServiceConfiguration kafkaConfig,
                              KopRequestManager requestManager) {
        this.kafkaConfig = kafkaConfig;
        this.requestManager = requestManager;
        this.responseHandles = new ArrayList<>(kafkaConfig.getNumResponseThreads());
    }

    private void addResponseKopEventManager() {
        int maxNumResponseThreads = kafkaConfig.getNumResponseThreads();
        for (int i = 0; i < maxNumResponseThreads; i++) {
            responseHandles.add(new KopEventManager(null,
                    null,
                    null,
                    "kop-response-thread-" + i));
        }
    }

    public void start() {
        addResponseKopEventManager();
        responseHandles.forEach(KopEventManager::start);
    }

    public void close() {
        responseHandles.forEach(KopEventManager::close);
    }

    public void addResponse(Channel channel,
                            KafkaCommandDecoder.ResponseAndRequest responseAndRequest,
                            RequestStats requestStats) {
        if (requestManager.getChannels().containsKey(channel)) {
            KopEventManager eventManager = getResponseKopEventManager(channel);
            eventManager.put(eventManager.getKopResponseEvent(channel,
                    responseAndRequest,
                    requestStats,
                    kafkaConfig.getRequestTimeoutMs()));
            if (log.isDebugEnabled()) {
                log.debug("AddResponse for request {} to kopEventManager {}, channel {}",
                        responseAndRequest.getRequest(), eventManager.getKopEventThreadName(), channel);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("AddResponse for request {} failed, because channel {} not active.",
                        responseAndRequest.getRequest(), channel);
            }
        }
    }

    private KopEventManager getResponseKopEventManager(Channel channel) {
        String channelId = requestManager.getChannels().get(channel);
        int responseHandleIndex = Math.abs(channelId.hashCode()) % responseHandles.size();
        return responseHandles.get(responseHandleIndex);
    }

    public static ByteBuf responseToByteBuf(AbstractResponse response,
                                            KafkaCommandDecoder.KafkaHeaderAndRequest request) {
        try (KafkaCommandDecoder.KafkaHeaderAndResponse kafkaHeaderAndResponse =
                     KafkaCommandDecoder.KafkaHeaderAndResponse.responseForRequest(request, response)) {
            // Lowering Client API_VERSION request to the oldest API_VERSION KoP supports, this is to make \
            // Kafka-Clients 2.4.x and above compatible and prevent KoP from panicking \
            // when it comes across a higher API_VERSION.
            short apiVersion = kafkaHeaderAndResponse.getApiVersion();
            if (request.getHeader().apiKey() == API_VERSIONS) {
                if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersion)) {
                    apiVersion = ApiKeys.API_VERSIONS.oldestVersion();
                }
            }
            return ResponseUtils.serializeResponse(
                    apiVersion,
                    kafkaHeaderAndResponse.getHeader(),
                    kafkaHeaderAndResponse.getResponse()
            );
        } finally {
            // the request is not needed any more.
            request.close();
        }
    }


}
