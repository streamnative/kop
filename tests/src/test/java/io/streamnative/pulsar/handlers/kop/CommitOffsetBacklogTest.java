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


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka produce messages.
 */
@Slf4j
public class CommitOffsetBacklogTest extends KopProtocolHandlerTestBase {

    @Override
    protected void resetConfig() {
        super.resetConfig();
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }

        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/default",
                new RetentionPolicies(60, 1000));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                new RetentionPolicies(-1, -1));
        }
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    // dump Topic Stats, mainly want to get backlogSize.
    private void dumpSubStats(PersistentTopic persistentTopic, int index) {
        TopicStats topicStats = persistentTopic.getStats();
        log.info("++++ dump index {}, dump topicStats for topic : {}, storageSize: {}, backlogSize: {}",
            index, persistentTopic.getName(),
            topicStats.storageSize, topicStats.backlogSize,
            topicStats.subscriptions.size());

        topicStats.subscriptions.forEach((subname, substats) -> {
            log.info("++++ 1. subname: {}, activeConsumerName {}, consumers {}, msgBacklog {}, unackedMessages {}.",
                subname,
                substats.activeConsumerName, substats.consumers, substats.msgBacklog, substats.unackedMessages);
        });

        persistentTopic.getManagedLedger().getCursors().forEach(cursor ->
            log.info(" ++++ 2. cursor: {}, durable: {}, numberEntryis: {}, readPosition: {}, markdeletePosition: {}",
                cursor.getName(), cursor.isDurable(), cursor.getNumberOfEntries(), cursor.getReadPosition(),
                cursor.getMarkDeletedPosition()));
    }

    private Map<TopicPartition, Long> getKConsumerOffset(KConsumer kConsumer, List<TopicPartition> partitions) {
        Map<TopicPartition, Long> offsets = kConsumer.getConsumer().endOffsets(partitions);
        offsets.forEach((topic, offset) -> log.info("++++ topic: {} offset: {}", topic, offset));
        return offsets;
    }

    @Test(timeOut = 20000)
    public void testOffsetCommittedBacklogCleared() throws Exception {
        String kafkaTopicName = "kopOffsetCommittedBacklogCleared";
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;
        String pulsarPartitionName = pulsarTopicName + "-partition-" + 0;

        TopicPartition kafkaPartition = new TopicPartition(kafkaTopicName, 0);
        List<TopicPartition> kafkaPartitions = Lists.newArrayList(kafkaPartition);

        // create partitioned topic with 1 partition.
        admin.topics().createPartitionedTopic(kafkaTopicName, 1);

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(pulsarPartitionName)
            .receiverQueueSize(0)
            .subscriptionName("kopOffsetCommittedBacklogCleared_sub")
            .subscribe();

        PersistentTopic topicRef = (PersistentTopic)
            pulsar.getBrokerService().getTopicReference(pulsarPartitionName).get();

        // 1. produce message with Kafka producer. todo: turn to pulsar producer to easy get messageid.
        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort());
        int totalMsgs = 50;
        String messageStrPrefix = "Message_Kop_ProduceConsumeMultiLedger_XXXXXXXXXXXXXXXX_";
        // send in sync mode, each message not batched.
        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                kafkaTopicName,
                i,
                messageStr);

            kProducer.getProducer().send(record).get();

            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        dumpSubStats(topicRef, 1);

        // 2. use kafka consumer to consume, use consumer group, offset auto-commit
        String consumerGroupName = kafkaTopicName + "_cg_a";
        @Cleanup
        KConsumer kConsumer = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), false, consumerGroupName);
        kConsumer.getConsumer().subscribe(Collections.singletonList(kafkaTopicName));

        int i = 0;
        while (i < totalMsgs / 2) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka Consumer Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
                i++;
            }
        }

        Map<TopicPartition, Long> offsets = getKConsumerOffset(kConsumer, kafkaPartitions);
        long offset = offsets.get(kafkaPartition);

        MessageId messageId = MessageIdUtils.getMessageId(offset);

        dumpSubStats(topicRef, 2);

        kConsumer.getConsumer().commitSync();
        Thread.sleep(1000);

        dumpSubStats(topicRef, 3);
               consumer.acknowledgeCumulative(messageId);
        Thread.sleep(1000);

        dumpSubStats(topicRef, 4);

    }



}
