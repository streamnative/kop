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
package io.streamnative.pulsar.handlers.kop.compatibility;

import io.streamnative.kafka.client.api.ConsumerConfiguration;
import io.streamnative.kafka.client.api.KafkaClientFactory;
import io.streamnative.kafka.client.api.KafkaClientFactoryImpl;
import io.streamnative.kafka.client.api.KafkaVersion;
import io.streamnative.kafka.client.api.ProducerConfiguration;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Basic end-to-end test for different versions of Kafka clients.
 */
public class BasicEndToEndTestBase extends KopProtocolHandlerTestBase {

    protected Map<KafkaVersion, KafkaClientFactory> kafkaClientFactories = Arrays.stream(KafkaVersion.values())
            .collect(Collectors.toMap(
                    version -> version,
                    version -> {
                        if (version.equals(KafkaVersion.DEFAULT)) {
                            return new DefaultKafkaClientFactory();
                        } else {
                            return new KafkaClientFactoryImpl(version);
                        }
                    },
                    (k, v) -> {
                        throw new IllegalStateException("Duplicated key: " + k);
                    }, TreeMap::new));

    public BasicEndToEndTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected ProducerConfiguration producerConfiguration(final KafkaVersion version) {
        return ProducerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .keySerializer(version.getStringSerializer())
                .valueSerializer(version.getStringSerializer())
                .build();
    }

    protected ConsumerConfiguration consumerConfiguration(final KafkaVersion version) {
        return ConsumerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .groupId("group-" + version.name())
                .keyDeserializer(version.getStringDeserializer())
                .valueDeserializer(version.getStringDeserializer())
                .fromEarliest(true)
                .build();
    }
}
