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

import java.nio.ByteBuffer;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;

/**
 * The client that is used to create producers and readers for system topics like __consumer_offsets.
 */
public class SystemTopicClient extends AbstractPulsarClient {

    private final int maxPendingMessages;

    public SystemTopicClient(final PulsarService pulsarService, final KafkaServiceConfiguration kafkaConfig) {
        // Disable stats recorder for producer and readers
        super(createPulsarClient(pulsarService, kafkaConfig, conf -> conf.setStatsIntervalSeconds(0L)));
        maxPendingMessages = kafkaConfig.getKafkaMetaMaxPendingMessages();
    }

    public ProducerBuilder<ByteBuffer> newProducerBuilder() {
        return getPulsarClient().newProducer(Schema.BYTEBUFFER)
                .maxPendingMessages(maxPendingMessages)
                .blockIfQueueFull(true);
    }

    public ReaderBuilder<ByteBuffer> newReaderBuilder() {
        return getPulsarClient().newReader(Schema.BYTEBUFFER)
                .startMessageId(MessageId.earliest);
    }
}
