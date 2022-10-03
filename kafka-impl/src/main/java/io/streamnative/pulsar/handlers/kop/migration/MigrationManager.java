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
package io.streamnative.pulsar.handlers.kop.migration;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.http.HttpChannelInitializer;
import io.streamnative.pulsar.handlers.kop.http.HttpHandler;
import io.streamnative.pulsar.handlers.kop.migration.processor.CreateTopicWithMigrationProcessor;
import io.streamnative.pulsar.handlers.kop.migration.processor.MigrationStatusProcessor;
import io.streamnative.pulsar.handlers.kop.migration.processor.StartMigrationProcessor;
import io.streamnative.pulsar.handlers.kop.migration.requests.CreateTopicWithMigrationRequest;
import io.streamnative.pulsar.handlers.kop.migration.requests.StartMigrationRequest;
import java.net.InetSocketAddress;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * The MigrationManager class manages Kafka to KoP topic migrations.
 */
@Slf4j
public class MigrationManager {
    private final KafkaServiceConfiguration kafkaConfig;
    private final PulsarClient pulsarClient;

    /**
     * Creates a MigrationManager.
     * @param kafkaConfig the KafkaConfig used by the underlying PulsarClient
     * @param pulsar the PulsarService
     */
    public MigrationManager(KafkaServiceConfiguration kafkaConfig,
                            PulsarService pulsar) {
        this.kafkaConfig = kafkaConfig;
        this.pulsarClient = SystemTopicClient.createPulsarClient(pulsar, kafkaConfig, (___) -> {});
    }

    /**
     * Get the address of the KoP migration service.
     * @return the address of the KoP migration service
     */
    public InetSocketAddress getAddress() {
        return new InetSocketAddress(kafkaConfig.getKopMigrationServicePort());
    }

    /**
     * Build an HttpChannelInitializer for KoP migration service.
     * @return the HttpChannelInitializer for KoP migration service
     */
    public Optional<HttpChannelInitializer> build() {
        if (!kafkaConfig.isKopMigrationEnable()) {
            return Optional.empty();
        }
        HttpHandler handler = new MigrationHandler();
        handler.addProcessor(new CreateTopicWithMigrationProcessor(CreateTopicWithMigrationRequest.class));
        handler.addProcessor(new StartMigrationProcessor(StartMigrationRequest.class));
        handler.addProcessor(new MigrationStatusProcessor(Void.class));

        return Optional.of(new HttpChannelInitializer(handler));
    }

    /**
     * Close and clean up resource usage.
     */
    public void close() {
        try {
            pulsarClient.close();
        } catch (PulsarClientException err) {
            log.error("Error while shutting down", err);
        }
    }
}
