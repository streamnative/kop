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


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * System topic client factory.
 */
@Slf4j
public class SystemTopicClientFactory {

    public static final String SYS_TOPIC_PRODUCER_STATE = "__producer_state";

    private final ProducerStateSystemTopicClient systemTopicClient;

    private final Map<TopicName, SystemTopicProducerStateClient> producerStateClientMap = Maps.newConcurrentMap();

    private final int kafkaProducerStateTopicNumPartitions;

    public SystemTopicClientFactory(ProducerStateSystemTopicClient systemTopicClient,
                                    int kafkaProducerStateTopicNumPartitions) {
        this.systemTopicClient = systemTopicClient;
        this.kafkaProducerStateTopicNumPartitions = kafkaProducerStateTopicNumPartitions;
    }

    public TopicName getProducerStateTopicName(String topic) {
        TopicName topicName = TopicName.get(topic);
        return TopicName.get(
                TopicDomain.persistent.value(),
                topicName.getTenant(),
                topicName.getNamespacePortion(),
                SYS_TOPIC_PRODUCER_STATE);
    }

    public SystemTopicProducerStateClient getProducerStateClient(String topic) {
        TopicName sysTopicName = getProducerStateTopicName(topic);
        return producerStateClientMap.computeIfAbsent(sysTopicName, key ->
                new SystemTopicProducerStateClient(
                        systemTopicClient, TopicName.get(topic), sysTopicName, kafkaProducerStateTopicNumPartitions));
    }

    public void shutdown() {
        List<CompletableFuture<Void>> allFutures = Lists.newArrayList();
        producerStateClientMap.forEach((topic, client) -> {
            CompletableFuture<Void> completableFuture = client.closeAsync();
            allFutures.add(completableFuture);
        });
        FutureUtil.waitForAll(allFutures).thenAccept(__ -> {
            if (log.isDebugEnabled()) {
                log.debug("Closed all the {} producerStateClient in SystemTopicClientFactory.",
                        producerStateClientMap.size());
            }
        }).exceptionally(ex -> {
            if (ex != null) {
                log.error("Error when close all the {} producerStateClient in SystemTopicClientFactory",
                        producerStateClientMap.size(), ex);
            }
            return null;
        });
    }
}
