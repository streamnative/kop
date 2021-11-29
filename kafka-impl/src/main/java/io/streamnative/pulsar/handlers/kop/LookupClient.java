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

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;

/**
 * The client that is responsible for topic lookup.
 */
@Slf4j
public class LookupClient extends AbstractPulsarClient {

    public LookupClient(final PulsarService pulsarService, final KafkaServiceConfiguration kafkaConfig) {
        super(createPulsarClient(pulsarService, kafkaConfig, conf -> {}));
    }

    public LookupClient(final PulsarService pulsarService) {
        super(createPulsarClient(pulsarService));
        log.warn("This constructor should not be called, it's only called "
                + "when the PulsarService doesn't exist in KafkaProtocolHandlers.LOOKUP_CLIENT_UP");
    }

    public CompletableFuture<InetSocketAddress> getBrokerAddress(final TopicName topicName) {
        CompletableFuture<InetSocketAddress> res =  getPulsarClient().getLookup().getBroker(topicName).thenApply(Pair::getLeft);
        res.whenComplete((r, t) -> {
            log.info("getBrokerAddress for {} is {}", topicName, r);
        });
        return res;
    }
}
