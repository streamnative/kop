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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.migration.metadata.MigrationMetadataManager;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.testng.annotations.Test;

public class MigrationManagerTest {

    @Test
    public void testGetAddress() {
        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getBrokerServiceUrl()).thenReturn("http://localhost");
        KafkaServiceConfiguration kafkaServiceConfiguration = new KafkaServiceConfiguration();
        int port = 8005;
        kafkaServiceConfiguration.setKopMigrationServicePort(port);
        MigrationMetadataManager migrationMetadataManager = mock(MigrationMetadataManager.class);
        MigrationManager migrationManager =
                new MigrationManager(kafkaServiceConfiguration, pulsarService, migrationMetadataManager);
        assertEquals(migrationManager.getAddress(), new InetSocketAddress(port));
    }

    @Test
    public void testBuild() {
        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getBrokerServiceUrl()).thenReturn("http://localhost");
        KafkaServiceConfiguration kafkaServiceConfiguration = new KafkaServiceConfiguration();
        kafkaServiceConfiguration.setKopMigrationEnable(true);
        MigrationMetadataManager migrationMetadataManager = mock(MigrationMetadataManager.class);
        MigrationManager migrationManager =
                new MigrationManager(kafkaServiceConfiguration, pulsarService, migrationMetadataManager);
        assertTrue(migrationManager.build().isPresent());
    }

    @Test
    public void testBuildReturnsEmptyWhenMigrationIsDisabled() {
        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getBrokerServiceUrl()).thenReturn("http://localhost");
        KafkaServiceConfiguration kafkaServiceConfiguration = new KafkaServiceConfiguration();
        kafkaServiceConfiguration.setKopMigrationEnable(false);
        MigrationMetadataManager migrationMetadataManager = mock(MigrationMetadataManager.class);
        MigrationManager migrationManager =
                new MigrationManager(kafkaServiceConfiguration, pulsarService, migrationMetadataManager);
        assertEquals(migrationManager.build(), Optional.empty());
    }
}
