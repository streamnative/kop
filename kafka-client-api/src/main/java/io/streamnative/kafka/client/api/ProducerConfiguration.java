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
 * The configuration of Kafka producer.
 */
@Builder
public class ProducerConfiguration {

    private String bootstrapServers;
    private Object keySerializer;
    private Object valueSerializer;
    private String maxBlockMs;
    private String securityProtocol;
    private String saslMechanism;
    private String userName;
    private String password;

    public Properties toProperties() {
        final Properties props = new Properties();
        if (bootstrapServers != null) {
            props.put("bootstrap.servers", bootstrapServers);
        }
        if (keySerializer != null) {
            props.put("key.serializer", keySerializer);
        }
        if (valueSerializer != null) {
            props.put("value.serializer", valueSerializer);
        }
        if (maxBlockMs != null) {
            props.put("max.block.ms", maxBlockMs);
        }
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
        return props;
    }
}
