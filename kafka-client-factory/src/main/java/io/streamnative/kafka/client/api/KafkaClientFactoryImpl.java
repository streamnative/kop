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
package io.streamnative.kafka.client.api;

import io.streamnative.kafka.client.one.zero.ConsumerImpl;
import io.streamnative.kafka.client.one.zero.ProducerImpl;
import io.streamnative.kafka.client.three.zero.Consumer300Impl;
import io.streamnative.kafka.client.three.zero.Producer300Impl;
import io.streamnative.kafka.client.two.eight.Consumer280Impl;
import io.streamnative.kafka.client.two.eight.Producer280Impl;
import io.streamnative.kafka.client.zero.nine.Consumer009Impl;
import io.streamnative.kafka.client.zero.nine.Producer009Impl;
import io.streamnative.kafka.client.zero.ten.Consumer010Impl;
import io.streamnative.kafka.client.zero.ten.Producer010Impl;

/**
 * The factory class to create Kafka producers or consumers with a specific version.
 */
public class KafkaClientFactoryImpl implements KafkaClientFactory {

    private KafkaVersion kafkaVersion;

    public KafkaClientFactoryImpl(final KafkaVersion kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
    }

    @Override
    public <K, V> Producer<K, V> createProducer(final ProducerConfiguration conf) {
        if (kafkaVersion.equals(KafkaVersion.KAFKA_1_0_0)) {
            return new ProducerImpl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_0_10_0_0)) {
            return new Producer010Impl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_2_8_0)) {
            return new Producer280Impl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_3_0_0)) {
            return new Producer300Impl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_0_9_0_0)) {
            return new Producer009Impl<>(conf);
        }
        throw new IllegalArgumentException("No producer for version: " + kafkaVersion);
    }

    @Override
    public <K, V> Consumer<K, V> createConsumer(final ConsumerConfiguration conf) {
        if (kafkaVersion.equals(KafkaVersion.KAFKA_1_0_0)) {
            return new ConsumerImpl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_0_10_0_0)) {
            return new Consumer010Impl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_2_8_0)) {
            return new Consumer280Impl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_3_0_0)) {
            return new Consumer300Impl<>(conf);
        } else if (kafkaVersion.equals(KafkaVersion.KAFKA_0_9_0_0)) {
            return new Consumer009Impl<>(conf);
        }
        throw new IllegalArgumentException("No consumer for version: " + kafkaVersion);
    }
}
