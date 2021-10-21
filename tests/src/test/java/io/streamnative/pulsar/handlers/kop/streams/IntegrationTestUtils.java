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

import static io.streamnative.pulsar.handlers.kop.streams.TestUtils.retryOnExceptionWithTimeout;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.fail;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.ThreadStateTransitionValidator;
import org.hamcrest.Matchers;

@Slf4j
public class IntegrationTestUtils {

    public static final long DEFAULT_TIMEOUT = 60 * 1000L;

    /*
     * Records state transition for StreamThread
     */
    public static class StateListenerStub implements StreamThread.StateListener {
        boolean toPendingShutdownSeen = false;

        @Override
        public void onChange(final Thread thread,
                             final ThreadStateTransitionValidator newState,
                             final ThreadStateTransitionValidator oldState) {
            if (newState == StreamThread.State.PENDING_SHUTDOWN) {
                toPendingShutdownSeen = true;
            }
        }

        public boolean transitToPendingShutdownSeen() {
            return toPendingShutdownSeen;
        }
    }

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param time           Timestamp provider
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
            final String topic, final Collection<KeyValue<K, V>> records, final Properties producerConfig,
            final Time time)
            throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronously(topic, records, producerConfig, time, false);
    }

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param headers        {@link Headers} of the data records
     * @param time           Timestamp provider
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
            final String topic, final Collection<KeyValue<K, V>> records, final Properties producerConfig,
            final Headers headers, final Time time)
            throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronously(topic, records, producerConfig, headers, time, false);
    }

    /**
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param producerConfig     Kafka producer configuration
     * @param time               Timestamp provider
     * @param enableTransactions Send messages in a transaction
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
            final String topic, final Collection<KeyValue<K, V>> records, final Properties producerConfig,
            final Time time, final boolean enableTransactions)
            throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronously(topic, records, producerConfig, null, time, enableTransactions);
    }

    /**
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param producerConfig     Kafka producer configuration
     * @param headers            {@link Headers} of the data records
     * @param time               Timestamp provider
     * @param enableTransactions Send messages in a transaction
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
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

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param timestamp      Timestamp of the record
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp)
            throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, timestamp, false);
    }

    /**
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param producerConfig     Kafka producer configuration
     * @param timestamp          Timestamp of the record
     * @param enableTransactions Send messages in a transaction
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
    @SuppressWarnings("WeakerAccess")
    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp,
                                                                         final boolean enableTransactions)
            throws ExecutionException, InterruptedException {

        produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, null, timestamp, enableTransactions);
    }

    /**
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param producerConfig     Kafka producer configuration
     * @param headers            {@link Headers} of the data records
     * @param timestamp          Timestamp of the record
     * @param enableTransactions Send messages in a transaction
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
    @SuppressWarnings("WeakerAccess")
    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Headers headers,
                                                                         final Long timestamp,
                                                                         final boolean enableTransactions)
            throws ExecutionException, InterruptedException {

        try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            if (enableTransactions) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            for (final KeyValue<K, V> record : records) {
                final Future<RecordMetadata> f = producer.send(
                        new ProducerRecord<>(topic, null, timestamp, record.key, record.value, headers));
                f.get();
            }
            if (enableTransactions) {
                producer.commitTransaction();
            }
            producer.flush();
        }
    }

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param time           Timestamp provider
     * @param <V>            Value type of the data records
     */
    public static <V> void produceValuesSynchronously(final String topic,
                                                      final Collection<V> records,
                                                      final Properties producerConfig,
                                                      final Time time)
            throws ExecutionException, InterruptedException {
        produceValuesSynchronously(topic, records, producerConfig, time, false);
    }

    /**
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param producerConfig     Kafka producer configuration
     * @param time               Timestamp provider
     * @param enableTransactions Send messages in a transaction
     * @param <V>                Value type of the data records
     */
    @SuppressWarnings("WeakerAccess")
    public static <V> void produceValuesSynchronously(final String topic,
                                                      final Collection<V> records,
                                                      final Properties producerConfig,
                                                      final Time time,
                                                      final boolean enableTransactions)
            throws ExecutionException, InterruptedException {
        final Collection<KeyValue<Object, V>> keyedRecords = new ArrayList<>();
        for (final V value : records) {
            final KeyValue<Object, V> kv = new KeyValue<>(null, value);
            keyedRecords.add(kv);
        }
        produceKeyValuesSynchronously(topic, keyedRecords, producerConfig, time, enableTransactions);
    }

    /**
     * Wait for streams to "finish", based on the consumer lag metric.
     *
     * Caveats:
     * - Inputs must be finite, fully loaded, and flushed before this method is called
     * - expectedPartitions is the total number of partitions to watch the lag on, including both input and internal.
     * It's somewhat ok to get this wrong, as the main failure case would be an immediate return due to the clients
     * not being initialized, which you can avoid with any non-zero value. But it's probably better to get it right ;)
     */
    public static void waitForCompletion(final KafkaStreams streams,
                                         final int expectedPartitions,
                                         final int timeoutMilliseconds) {
        final long start = System.currentTimeMillis();
        while (true) {
            int lagMetrics = 0;
            double totalLag = 0.0;
            for (final Metric metric : streams.metrics().values()) {
                if (metric.metricName().name().equals("records-lag")) {
                    lagMetrics++;
                    totalLag += ((Number) metric.metricValue()).doubleValue();
                }
            }
            if (lagMetrics >= expectedPartitions && totalLag == 0.0) {
                return;
            }
            if (System.currentTimeMillis() - start >= timeoutMilliseconds) {
                throw new RuntimeException(String.format(
                        "Timed out waiting for completion. lagMetrics=[%s/%s] totalLag=[%s]",
                        lagMetrics, expectedPartitions, totalLag
                ));
            }
        }
    }

    /**
     * Wait until enough data (consumer records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     */
    @SuppressWarnings("WeakerAccess")
    public static <K, V> List<ConsumerRecord<K, V>> waitUntilMinRecordsReceived(final Properties consumerConfig,
                                                                                final String topic,
                                                                                final int expectedNumRecords)
            throws InterruptedException {
        return waitUntilMinRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (consumer records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound of waiting time in milliseconds
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     */
    @SuppressWarnings("WeakerAccess")
    public static <K, V> List<ConsumerRecord<K, V>> waitUntilMinRecordsReceived(final Properties consumerConfig,
                                                                                final String topic,
                                                                                final int expectedNumRecords,
                                                                                final long waitTime)
            throws InterruptedException {
        final List<ConsumerRecord<K, V>> accumData = new ArrayList<>();
        final String reason =
                String.format("Did not receive all %d records from topic %s within %d ms", expectedNumRecords, topic,
                        waitTime);
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            retryOnExceptionWithTimeout(waitTime, () -> {
                final List<ConsumerRecord<K, V>> readData =
                        readRecords(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                assertThat(reason, accumData.size(), Matchers.is(greaterThanOrEqualTo(expectedNumRecords)));
            });
        }
        return accumData;
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords)
            throws InterruptedException {
        return waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound of waiting time in milliseconds
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords,
                                                                                  final long waitTime)
            throws InterruptedException {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();
        final String reason =
                String.format("Did not receive all %d records from topic %s within %d ms", expectedNumRecords, topic,
                        waitTime);
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            retryOnExceptionWithTimeout(waitTime, () -> {
                final List<KeyValue<K, V>> readData =
                        readKeyValues(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                assertThat(reason, accumData.size(), Matchers.is(greaterThanOrEqualTo(expectedNumRecords)));
            });
        }
        return accumData;
    }

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound of waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords,
                                                                final long waitTime) throws InterruptedException {
        final List<V> accumData = new ArrayList<>();
        final String reason =
                String.format("Did not receive all %d records from topic %s within %d ms", expectedNumRecords, topic,
                        waitTime);
        try (final Consumer<Object, V> consumer = createConsumer(consumerConfig)) {
            retryOnExceptionWithTimeout(waitTime, () -> {
                final List<V> readData =
                        readValues(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                assertThat(reason, accumData.size(), Matchers.is(greaterThanOrEqualTo(expectedNumRecords)));
            });
        }
        return accumData;
    }

    /**
     * Starts the given {@link KafkaStreams} instances and waits for all of them to reach the
     * {@link KafkaStreams.State#RUNNING} state at the same time. Note that states may change between the time
     * that this method returns and the calling function executes its next statement.<p>
     *
     * When the application is already started use {@link #waitForApplicationState(List, KafkaStreams.State, Duration)}
     * to wait for instances to reach {@link KafkaStreams.State#RUNNING} state.
     *
     * @param streamsList the list of streams instances to run.
     * @param timeout     the time to wait for the streams to all be in {@link KafkaStreams.State#RUNNING} state.
     */
    public static void startApplicationAndWaitUntilRunning(final List<KafkaStreams> streamsList,
                                                           final Duration timeout) throws InterruptedException {
        final Lock stateLock = new ReentrantLock();
        final Condition stateUpdate = stateLock.newCondition();
        final Map<KafkaStreams, KafkaStreams.State> stateMap = new HashMap<>();
        for (final KafkaStreams streams : streamsList) {
            stateMap.put(streams, streams.state());
            final KafkaStreams.StateListener prevStateListener = getStateListener(streams);
            final KafkaStreams.StateListener newStateListener = (newState, oldState) -> {
                stateLock.lock();
                try {
                    stateMap.put(streams, newState);
                    if (newState == KafkaStreams.State.RUNNING) {
                        if (stateMap.values().stream().allMatch(state -> state == KafkaStreams.State.RUNNING)) {
                            stateUpdate.signalAll();
                        }
                    }
                } finally {
                    stateLock.unlock();
                }
            };

            streams.setStateListener(prevStateListener != null
                    ? new CompositeStateListener(prevStateListener, newStateListener)
                    : newStateListener);
        }

        for (final KafkaStreams streams : streamsList) {
            streams.start();
        }

        final long expectedEnd = System.currentTimeMillis() + timeout.toMillis();
        stateLock.lock();
        try {
            // We use while true here because we want to run this test at least once, even if the
            // timeout has expired
            while (true) {
                final Map<KafkaStreams, KafkaStreams.State> nonRunningStreams = new HashMap<>();
                for (final Map.Entry<KafkaStreams, KafkaStreams.State> entry : stateMap.entrySet()) {
                    if (entry.getValue() != KafkaStreams.State.RUNNING) {
                        nonRunningStreams.put(entry.getKey(), entry.getValue());
                    }
                }

                if (nonRunningStreams.isEmpty()) {
                    return;
                }

                final long millisRemaining = expectedEnd - System.currentTimeMillis();
                if (millisRemaining <= 0) {
                    fail("Application did not reach a RUNNING state for all streams instances. Non-running instances: "
                            +
                            nonRunningStreams);
                }

                stateUpdate.await(millisRemaining, TimeUnit.MILLISECONDS);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Waits for the given {@link KafkaStreams} instances to all be in a  {@link KafkaStreams.State#RUNNING}
     * state. Prefer {@link #startApplicationAndWaitUntilRunning(List, Duration)} when possible
     * because this method uses polling, which can be more error prone and slightly slower.
     *
     * @param streamsList the list of streams instances to run.
     * @param timeout     the time to wait for the streams to all be in {@link KafkaStreams.State#RUNNING} state.
     */
    public static void waitForApplicationState(final List<KafkaStreams> streamsList,
                                               final KafkaStreams.State state,
                                               final Duration timeout) throws InterruptedException {
        retryOnExceptionWithTimeout(timeout.toMillis(), () -> {
            final Map<KafkaStreams, KafkaStreams.State> streamsToStates = streamsList
                    .stream()
                    .collect(Collectors.toMap(stream -> stream, KafkaStreams::state));

            final Map<KafkaStreams, KafkaStreams.State> wrongStateMap = streamsToStates.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != state)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final String reason = String.format(
                    "Expected all streams instances in %s to be %s within %d ms, but the following were not: %s",
                    streamsList, state, timeout.toMillis(), wrongStateMap);
            assertThat(reason, wrongStateMap.isEmpty());
        });
    }

    private static KafkaStreams.StateListener getStateListener(final KafkaStreams streams) {
        try {
            final Field field = streams.getClass().getDeclaredField("stateListener");
            field.setAccessible(true);
            return (KafkaStreams.StateListener) field.get(streams);
        } catch (final IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Failed to get StateListener through reflection", e);
        }
    }


    private static <K, V> void compareKeyValueTimestamp(final ConsumerRecord<K, V> record,
                                                        final K expectedKey,
                                                        final V expectedValue,
                                                        final long expectedTimestamp) {
        Objects.requireNonNull(record);
        final K recordKey = record.key();
        final V recordValue = record.value();
        final long recordTimestamp = record.timestamp();
        final AssertionError error = new AssertionError(
                "Expected <" + expectedKey + ", " + expectedValue + "> with timestamp=" + expectedTimestamp +
                        " but was <" + recordKey + ", " + recordValue + "> with timestamp=" + recordTimestamp);
        if (recordKey != null) {
            if (!recordKey.equals(expectedKey)) {
                throw error;
            }
        } else if (expectedKey != null) {
            throw error;
        }
        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }
        if (recordTimestamp != expectedTimestamp) {
            throw error;
        }
    }

    private static <K, V> String printRecords(final List<ConsumerRecord<K, V>> result) {
        final StringBuilder resultStr = new StringBuilder();
        resultStr.append("[\n");
        for (final ConsumerRecord<?, ?> record : result) {
            resultStr.append("  ").append(record.toString()).append("\n");
        }
        resultStr.append("]");
        return resultStr.toString();
    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer config
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    public static <V> List<V> readValues(final String topic, final Properties consumerConfig,
                                         final long waitTime, final int maxMessages) {
        final List<V> returnList;
        try (final Consumer<Object, V> consumer = createConsumer(consumerConfig)) {
            returnList = readValues(topic, consumer, waitTime, maxMessages);
        }
        return returnList;
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer config
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic,
                                                            final Properties consumerConfig, final long waitTime,
                                                            final int maxMessages) {
        final List<KeyValue<K, V>> consumedValues;
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            consumedValues = readKeyValues(topic, consumer, waitTime, maxMessages);
        }
        return consumedValues;
    }

    public static KafkaStreams getStartedStreams(final Properties streamsConfig, final StreamsBuilder builder,
                                                 final boolean clean) {
        final KafkaStreams driver = new KafkaStreams(builder.build(), streamsConfig);
        if (clean) {
            driver.cleanUp();
        }
        driver.start();
        return driver;
    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic       Kafka topic to read messages from
     * @param consumer    Kafka consumer
     * @param waitTime    Maximum wait time in milliseconds
     * @param maxMessages Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    private static <V> List<V> readValues(final String topic,
                                          final Consumer<Object, V> consumer,
                                          final long waitTime,
                                          final int maxMessages) {
        final List<V> returnList = new ArrayList<>();
        final List<KeyValue<Object, V>> kvs = readKeyValues(topic, consumer, waitTime, maxMessages);
        for (final KeyValue<?, V> kv : kvs) {
            returnList.add(kv.value);
        }
        return returnList;
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic       Kafka topic to read messages from
     * @param consumer    Kafka consumer
     * @param waitTime    Maximum wait time in milliseconds
     * @param maxMessages Maximum number of messages to read via the consumer
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

    private static <K, V> List<ConsumerRecord<K, V>> readRecords(final String topic,
                                                                 final Consumer<K, V> consumer,
                                                                 final long waitTime,
                                                                 final int maxMessages) {
        final List<ConsumerRecord<K, V>> consumerRecords;
        consumer.subscribe(Collections.singletonList(topic));
        final int pollIntervalMs = 100;
        consumerRecords = new ArrayList<>();
        int totalPollTimeMs = 0;
        while (totalPollTimeMs < waitTime &&
                continueConsuming(consumerRecords.size(), maxMessages)) {
            totalPollTimeMs += pollIntervalMs;
            final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollIntervalMs));

            for (final ConsumerRecord<K, V> record : records) {
                log.info("readRecords received {}", record);
                consumerRecords.add(record);
            }
        }
        return consumerRecords;
    }

    private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    /**
     * Sets up a {@link KafkaConsumer} from a copy of the given configuration that has
     * {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} set to "earliest" and {@link
     * ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}
     * set to "true" to prevent missing events as well as repeat consumption.
     *
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

    public static class CompositeStateListener implements KafkaStreams.StateListener {
        private final List<KafkaStreams.StateListener> listeners;

        public CompositeStateListener(final KafkaStreams.StateListener... listeners) {
            this(Arrays.asList(listeners));
        }

        public CompositeStateListener(final Collection<KafkaStreams.StateListener> stateListeners) {
            this.listeners = Collections.unmodifiableList(new ArrayList<>(stateListeners));
        }

        @Override
        public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
            for (final KafkaStreams.StateListener listener : listeners) {
                listener.onChange(newState, oldState);
            }
        }
    }
}
