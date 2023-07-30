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
package io.streamnative.pulsar.handlers.kop.streams;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;
import java.time.Duration;
import java.util.Properties;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Base test class for tests related to Kafka Streams.
 */
public abstract class KafkaStreamsTestBase extends KopProtocolHandlerTestBase {
    protected final MockTime mockTime = new MockTime();
    protected String bootstrapServers;
    @Getter
    private int testNo = 0; // the suffix of the prefix of test topic name or application id, etc.
    private Properties streamsConfiguration;
    protected StreamsBuilder builder; // the builder to build `kafkaStreams` and other objects of Kafka Streams
    protected KafkaStreams kafkaStreams;

    public KafkaStreamsTestBase() {
        super("kafka");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        bootstrapServers = "localhost:" + getKafkaBrokerPort();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @BeforeMethod
    protected void setupTestCase() throws Exception {
        testNo++;
        createTopics();
        streamsConfiguration = new Properties();
        final String applicationId = getApplicationIdPrefix() + "-" + testNo;
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(TestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        if (getKeySerdeClass() != null) {
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, getKeySerdeClass());
        }
        if (getValueSerdeClass() != null) {
            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, getValueSerdeClass());
        }
        builder = new StreamsBuilder();
        extraSetup();
    }

    @AfterMethod
    protected void cleanupTestCase() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(3));
            TestUtils.purgeLocalStreamsState(streamsConfiguration);
        }
    }

    protected void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    protected abstract void createTopics() throws Exception;

    protected abstract @NonNull String getApplicationIdPrefix();

    protected abstract void extraSetup() throws Exception;

    protected abstract Class<?> getKeySerdeClass();

    protected abstract Class<?> getValueSerdeClass();
}
