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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
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

    // dump Topic Stats, mainly want to get and verify backlogSize.
    private void verifyBacklogInTopicStats(PersistentTopic persistentTopic, int expected) throws Exception {
        final AtomicLong backlog = new AtomicLong(0);
        retryStrategically(
            ((test) -> {
                backlog.set(persistentTopic.getStats(true).backlogSize);
                return backlog.get() == expected;
            }),
            5,
            200);

        if (log.isDebugEnabled()) {
            TopicStats topicStats = persistentTopic.getStats(true);
            log.info(" dump topicStats for topic : {}, storageSize: {}, backlogSize: {}, expected: {}",
                persistentTopic.getName(),
                topicStats.storageSize, topicStats.backlogSize, expected);

            topicStats.subscriptions.forEach((subname, substats) -> {
                log.debug(" dump sub: subname - {}, activeConsumerName {}, "
                        + "consumers {}, msgBacklog {}, unackedMessages {}.",
                    subname,
                    substats.activeConsumerName, substats.consumers,
                    substats.msgBacklog, substats.unackedMessages);
            });

            persistentTopic.getManagedLedger().getCursors().forEach(cursor ->
                log.debug(" dump cursor: cursor - {}, durable: {}, numberEntryis: {},"
                        + " readPosition: {}, markdeletePosition: {}",
                    cursor.getName(), cursor.isDurable(), cursor.getNumberOfEntries(),
                    cursor.getReadPosition(), cursor.getMarkDeletedPosition()));
        }

        assertEquals(backlog.get(), expected);
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

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(pulsarTopicName)
            .enableBatching(false);

        // create 1 pulsar producer and 2 kafka consumer group, each with 1 consumer.
        @Cleanup
        Producer<byte[]> producer = producerBuilder.create();
        PersistentTopic topicRef = (PersistentTopic)
            pulsar.getBrokerService().getTopicReference(pulsarPartitionName).get();

        String consumerGroupA = kafkaTopicName + "_cg_a";
        String consumerGroupB = kafkaTopicName + "_cg_b";
        @Cleanup
        KConsumer kConsumerA = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), false, consumerGroupA);
        @Cleanup
        KConsumer kConsumerB = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), false, consumerGroupB);

        kConsumerA.getConsumer().subscribe(Collections.singletonList(kafkaTopicName));
        kConsumerB.getConsumer().subscribe(Collections.singletonList(kafkaTopicName));

        int totalMsgs = 60;
        String messageStrPrefix = "Message_Kop_ProduceConsumeMultiLedger_XXXXXXXXXXXXX_";

        // create messages, each size of 100 bytes, all 6000 bytes.
        MessageId lastMessageId = null;
        for (int i = 0; i < totalMsgs; i++) {
            String message = messageStrPrefix + (i % 10);
            lastMessageId = producer.newMessage()
                .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
                .value(message.getBytes())
                .send();
        }

        int i = 0;
        while (i < totalMsgs / 2 - 1) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message from cgA: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumerA.getConsumer().poll(Duration.ofMillis(200));
            for (ConsumerRecord<Integer, String> record : records) {
                if (log.isDebugEnabled()) {
                    log.debug("Kafka ConsumerA Received message: {}, {} at offset {}",
                        record.key(), record.value(), record.offset());
                }
                i++;
            }
        }

        i = 0;
        while (i < totalMsgs / 2 - 1) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message from cgB: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumerB.getConsumer().poll(Duration.ofMillis(200));
            for (ConsumerRecord<Integer, String> record : records) {
                if (log.isDebugEnabled()) {
                    log.debug("Kafka ConsumerB Received message: {}, {} at offset {}",
                        record.key(), record.value(), record.offset());
                }
                i++;
            }
        }

        // 2 consumers not acked. expected backlog == 6000.
        verifyBacklogInTopicStats(topicRef, 6000);

        // 1 consumer acked. still expected backlog 6000
        kConsumerA.getConsumer().commitSync();
        verifyBacklogInTopicStats(topicRef, 6000);

        // 2 consumers acked, consumed 30 X 100. expected backlog 6000 - 3000
        kConsumerB.getConsumer().commitSync();
        verifyBacklogInTopicStats(topicRef, 6000 - 3000);

        // 2 consumers consumed and acked all messages, expected backlog 0.
        ConsumerRecords<Integer, String> recordsA = kConsumerA.getConsumer().poll(Duration.ofMillis(200));
        while (!recordsA.isEmpty()) {
            recordsA = kConsumerA.getConsumer().poll(Duration.ofMillis(200));
        }

        ConsumerRecords<Integer, String> recordsB = kConsumerB.getConsumer().poll(Duration.ofMillis(200));
        while (!recordsB.isEmpty()) {
            recordsB = kConsumerB.getConsumer().poll(Duration.ofMillis(200));
        }
        kConsumerA.getConsumer().commitSync();
        kConsumerB.getConsumer().commitSync();

        // wait for offsetAcker ack finished
        Thread.sleep(3000);
        verifyBacklogInTopicStats(topicRef, 0);
    }



}
