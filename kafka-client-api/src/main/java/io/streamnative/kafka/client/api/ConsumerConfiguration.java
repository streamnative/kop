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

import java.util.Properties;
import lombok.Builder;

/**
 * The configuration of Kafka consumer.
 */
@Builder
public class ConsumerConfiguration {

    private String bootstrapServers;
    private String groupId;
    private Object keyDeserializer;
    private Object valueDeserializer;
    @Builder.Default
    private boolean fromEarliest = true;
    private String securityProtocol;
    private String saslMechanism;
    private String userName;
    private String password;
    private String requestTimeoutMs;
    private Boolean enableAutoCommit;
    private String sessionTimeOutMs;

    public Properties toProperties() {
        final Properties props = new Properties();
        if (bootstrapServers != null) {
            props.put("bootstrap.servers", bootstrapServers);
        }
        if (groupId != null) {
            props.put("group.id", groupId);
        }
        if (keyDeserializer != null) {
            props.put("key.deserializer", keyDeserializer);
        }
        if (valueDeserializer != null) {
            props.put("value.deserializer", valueDeserializer);
        }
        props.put("auto.offset.reset", fromEarliest ? "earliest" : "latest");

        if (securityProtocol != null) {
            props.put("security.protocol", securityProtocol);
        }
        if (saslMechanism != null) {
            props.put("sasl.mechanism", saslMechanism);
        }
        if (userName != null && password != null) {
            final String kafkaAuth = String.format("username=\"%s\" password=\"%s\";", userName, password);
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + kafkaAuth);
        }
        if (requestTimeoutMs != null) {
            props.put("request.timeout.ms", requestTimeoutMs);
        }
        if (enableAutoCommit != null) {
            props.put("enable.auto.commit", enableAutoCommit);
        }
        if (sessionTimeOutMs != null) {
            props.put("session.timeout.ms", sessionTimeOutMs);
        }
        return props;
    }
}
