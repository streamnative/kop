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
package io.streamnative.pulsar.handlers.kop.systopic;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;


@Slf4j
public class SystemTopicProducerStateClient extends SystemTopicClientBase<ByteBuffer> {

    public static final String TOPIC_NAME_PROP = "topic_name";

    private final io.streamnative.pulsar.handlers.kop.SystemTopicClient systemTopicClient;

    private final TopicName sysTopicName;

    private final int kafkaProducerStateTopicNumPartitions;

    public SystemTopicProducerStateClient(SystemTopicClient systemTopicClient,
                                          TopicName userTopicName,
                                          TopicName sysTopicName, int kafkaProducerStateTopicNumPartitions) {
        super(null, userTopicName);
        this.sysTopicName = sysTopicName;
        this.systemTopicClient = systemTopicClient;
        this.kafkaProducerStateTopicNumPartitions = kafkaProducerStateTopicNumPartitions;
    }

    @Override
    protected CompletableFuture<Writer<ByteBuffer>> newWriterAsyncInternal() {
        String partitionTopic = sysTopicName.getPartition(
                MathUtils.signSafeMod(this.topicName.hashCode(), kafkaProducerStateTopicNumPartitions)).toString();
        return systemTopicClient.newProducerBuilder()
                .topic(partitionTopic)
                .createAsync()
                .thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("New system topic writer for topic {}", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new SystemTopicProducerStateWriter(this, producer));
                });
    }

    @Override
    protected CompletableFuture<Reader<ByteBuffer>> newReaderAsyncInternal() {
        String partitionTopic = sysTopicName.getPartition(
                MathUtils.signSafeMod(this.topicName.hashCode(), kafkaProducerStateTopicNumPartitions)).toString();
        return systemTopicClient.newReaderBuilder()
                .topic(partitionTopic)
                .readCompacted(true)
                .startMessageId(MessageId.earliest)
                .createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("New system topic reader for topic {}", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new SystemTopicProducerStateReader(this, reader));
                });
    }

}
