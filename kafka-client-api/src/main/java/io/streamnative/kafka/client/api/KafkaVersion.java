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

import lombok.Getter;

/**
 * The version that represents Kafka client's version.
 *
 * For each kafka-x.y.z module, the class path is relocated from `org.apache.kafka` to `org.apache.kafka-x-y-z` package.
 * See each module's pom.xml for details. This class provides convenient methods to get the actual class path for
 * different Kafka versions.
 */
public enum KafkaVersion {

    DEFAULT("default"), KAFKA_3_0_0("300"), KAFKA_2_8_0("280"),
    KAFKA_1_0_0("100"), KAFKA_0_10_0_0("010"), KAFKA_0_9_0_0("009");

    @Getter
    private String name;

    KafkaVersion(final String name) {
        this.name = name;
    }

    public String getStringSerializer() {
        if (this.equals(DEFAULT)) {
            return "org.apache.kafka.common.serialization.StringSerializer";
        }
        return String.format("org.apache.kafka%s.common.serialization.StringSerializer", name);
    }

    public String getStringDeserializer() {
        if (this.equals(DEFAULT)) {
            return "org.apache.kafka.common.serialization.StringDeserializer";
        }
        return String.format("org.apache.kafka%s.common.serialization.StringDeserializer", name);
    }

    // Because there is no header in ProducerRecord before 0.11.x.
    public boolean supportHeader() {
        return !equals(KAFKA_0_9_0_0) && !equals(KAFKA_0_10_0_0);
    }

    // Since Kafka supports SASL/PLAIN authentication from 0.10.0.0,
    // So skip 0.9.0.0 kafka client for sasl/plain test
    // see detail in https://kafka.apache.org/0100/documentation.html#security_overview
    public boolean supportSaslPlainAuthentication() {
        return !equals(KAFKA_0_9_0_0);
    }

}
