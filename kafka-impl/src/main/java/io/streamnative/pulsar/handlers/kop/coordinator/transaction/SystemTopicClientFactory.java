package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import com.google.common.collect.Maps;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

import java.util.Map;

public class SystemTopicClientFactory {

    private static final String SYS_TOPIC_PRODUCER_STATE = "sys-topic-producer-state";

    private final PulsarClient pulsarClient;

    private Map<TopicName, SystemTopicProducerStateClient> producerStateClientMap = Maps.newConcurrentMap();

    public SystemTopicClientFactory(PulsarClient pulsarClient) {
        this.pulsarClient = pulsarClient;
    }

    public TopicName getProducerStateTopicName(String topic) {
        TopicName topicName = TopicName.get(topic);
        return TopicName.get(
                TopicDomain.persistent.value(),
                topicName.getTenant(),
                topicName.getNamespacePortion(),
                SYS_TOPIC_PRODUCER_STATE);
    }

    public SystemTopicProducerStateClient generateProducerStateClient(String topic) {
        TopicName sysTopicName = getProducerStateTopicName(topic);
        return producerStateClientMap.computeIfAbsent(sysTopicName, key ->
                new SystemTopicProducerStateClient(pulsarClient, sysTopicName));
    }

}
