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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;


/**
 * Util methods for Kafka Streams tests.
 */
@Slf4j
public class TestUtils {

    public static final String INTERNAL_LEAVE_GROUP_ON_CLOSE = "internal.leave.group.on.close";

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed
     * to avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(
            final TestCondition testCondition, final long maxWaitMs, String conditionDetails)
            throws InterruptedException {
        final long startTime = System.currentTimeMillis();

        boolean testConditionMet;
        while (!(testConditionMet = testCondition.conditionMet())
                && ((System.currentTimeMillis() - startTime) < maxWaitMs)) {
            Thread.sleep(Math.min(maxWaitMs, 100L));
        }

        // don't re-evaluate testCondition.conditionMet() because this might slow down some tests significantly (this
        // could be avoided by making the implementations more robust, but we have a large number of such
        // implementations and it's easier to simply avoid the issue altogether)
        if (!testConditionMet) {
            conditionDetails = conditionDetails != null ? conditionDetails : "";
            throw new AssertionError("Condition not met within timeout " + maxWaitMs + ". " + conditionDetails);
        }
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError       if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords,
                                                                                  final long waitTime)
            throws InterruptedException {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            final TestCondition valuesRead = () -> {
                final List<KeyValue<K, V>> readData =
                        readKeyValues(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                return accumData.size() >= expectedNumRecords;
            };
            final String conditionDetails =
                    "Did not receive all " + expectedNumRecords + " records from topic " + topic;
            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
        }
        return accumData;
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError       if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, KeyValue<V, Long>>> waitUntilMinKeyValueWithTimestampRecordsReceived(
            final Properties consumerConfig,
            final String topic,
            final int expectedNumRecords,
            final long waitTime) throws InterruptedException {
        final List<KeyValue<K, KeyValue<V, Long>>> accumData = new ArrayList<>();
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            final TestCondition valuesRead = () -> {
                final List<KeyValue<K, KeyValue<V, Long>>> readData =
                        readKeyValuesWithTimestamp(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                return accumData.size() >= expectedNumRecords;
            };
            final String conditionDetails =
                    "Did not receive all " + expectedNumRecords + " records from topic " + topic;
            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
        }
        return accumData;
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist).
     *
     * @param file The root file at which to begin deleting
     */
    public static void delete(final File file) throws IOException {
        if (file == null) {
            return;
        }
        Files.walkFileTree(file.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                // If the root path did not exist, ignore the error; otherwise throw it.
                if (exc instanceof NoSuchFileException && path.toFile().equals(file)) {
                    return FileVisitResult.TERMINATE;
                }
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Create a temporary relative directory in the default temporary-file directory with a prefix of "kafka-".
     *
     * @return the temporary directory just created.
     */
    public static File tempDirectory() {
        final File file;
        try {
            file = Files.createTempDirectory("kafka-").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create a temp dir", e);
        }
        file.deleteOnExit();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        }));

        return file;
    }


    /**
     * Removes local state stores.  Useful to reset state in-between integration test runs.
     *
     * @param streamsConfiguration Streams configuration settings
     */
    public static void purgeLocalStreamsState(final Properties streamsConfiguration) throws
            IOException {
        final String tmpDir = new File(System.getProperty("java.io.tmpdir")).getPath();
        final String path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        if (path != null) {
            final File node = Paths.get(path).normalize().toFile();
            // Only purge state when it's under java.io.tmpdir.  This is a safety net to prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith(tmpDir)) {
                Utils.delete(new File(node.getAbsolutePath()));
            }
        }
    }

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
            final String topic,
            final Collection<KeyValue<K, V>> records,
            final Properties producerConfig,
            final Time time) throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronously(topic, records, producerConfig, time, false);
    }

    /**
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param producerConfig     Kafka producer configuration
     * @param enableTransactions Send messages in a transaction
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
            final String topic,
            final Collection<KeyValue<K, V>> records,
            final Properties producerConfig,
            final Time time,
            final boolean enableTransactions) throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronously(topic, records, producerConfig, null, time, enableTransactions);
    }

    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final Collection<KeyValue<K, V>> records,
                                                            final Properties producerConfig,
                                                            final Headers headers,
                                                            final Time time,
                                                            final boolean enableTransactions)
            throws ExecutionException, InterruptedException {
        for (final KeyValue<K, V> record : records) {
            produceKeyValuesSynchronouslyWithTimestamp(topic,
                    Collections.singleton(record),
                    producerConfig,
                    headers,
                    time.milliseconds(),
                    enableTransactions);
            time.sleep(1L);
        }
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp)
            throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, null, timestamp, false);
    }

    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Headers headers,
                                                                         final Long timestamp,
                                                                         final boolean enabledTransactions)
            throws ExecutionException, InterruptedException {
        try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            if (enabledTransactions) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            for (final KeyValue<K, V> record : records) {
                final Future<RecordMetadata> f = producer.send(
                        new ProducerRecord<>(topic, null, timestamp, record.key, record.value, headers));
                f.get();
            }
            if (enabledTransactions) {
                producer.commitTransaction();
            }
            producer.flush();
        }
    }

    public static Properties producerConfig(final String bootstrapServers,
                                            final Class keySerializer,
                                            final Class valueSerializer,
                                            final Properties additional) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.putAll(additional);
        return properties;
    }

    public static Properties producerConfig(final String bootstrapServers,
                                            final Class keySerializer,
                                            final Class valueSerializer) {
        return producerConfig(bootstrapServers, keySerializer, valueSerializer, new Properties());
    }

    /**
     * Sets up a {@link KafkaConsumer} from a copy of the given configuration that has
     * {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} set to "earliest" and
     * {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG} set to "true"
     * to prevent missing events as well as repeat consumption.
     * @param consumerConfig Consumer configuration
     * @return Consumer
     */
    private static <K, V> KafkaConsumer<K, V> createConsumer(final Properties consumerConfig) {
        final Properties filtered = new Properties();
        filtered.putAll(consumerConfig);
        filtered.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        filtered.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(filtered);
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumer       Kafka consumer
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    private static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic,
                                                             final Consumer<K, V> consumer,
                                                             final long waitTime,
                                                             final int maxMessages) {
        final List<KeyValue<K, V>> consumedValues = new ArrayList<>();
        final List<ConsumerRecord<K, V>> records = readRecords(topic, consumer, waitTime, maxMessages);
        for (final ConsumerRecord<K, V> record : records) {
            consumedValues.add(new KeyValue<>(record.key(), record.value()));
        }
        return consumedValues;
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumer       Kafka consumer
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    private static <K, V> List<KeyValue<K, KeyValue<V, Long>>> readKeyValuesWithTimestamp(final String topic,
                                                                                          final Consumer<K, V> consumer,
                                                                                          final long waitTime,
                                                                                          final int maxMessages) {
        final List<KeyValue<K, KeyValue<V, Long>>> consumedValues = new ArrayList<>();
        final List<ConsumerRecord<K, V>> records = readRecords(topic, consumer, waitTime, maxMessages);
        for (final ConsumerRecord<K, V> record : records) {
            consumedValues.add(new KeyValue<>(record.key(), KeyValue.pair(record.value(), record.timestamp())));
        }
        return consumedValues;
    }

    private static <K, V> List<ConsumerRecord<K, V>> readRecords(final String topic,
                                                                 final Consumer<K, V> consumer,
                                                                 final long waitTime,
                                                                 final int maxMessages) {
        final List<ConsumerRecord<K, V>> consumerRecords;
        consumer.subscribe(Collections.singletonList(topic));
        final int pollIntervalMs = 100;
        consumerRecords = new ArrayList<>();
        int totalPollTimeMs = 0;
        while (totalPollTimeMs < waitTime && continueConsuming(consumerRecords.size(), maxMessages)) {
            totalPollTimeMs += pollIntervalMs;
            final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollIntervalMs));

            for (final ConsumerRecord<K, V> record : records) {
                consumerRecords.add(record);
            }
        }
        return consumerRecords;
    }

    private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }
}
