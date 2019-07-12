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
package io.streamnative.kop;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * Pulsar service configuration object.
 */
public class KafkaServiceConfigurationTest {

    @Test
    public void testClusterName() {
        String clusterName = "kopCluster";
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setKafkaClusterName(clusterName);
        assertEquals(clusterName, configuration.getKafkaClusterName());
    }

    @Test
    public void testKopNamespace() {
        String name = "koptenant/kopns";
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setKafkaNamespace(name);
        assertEquals(name, configuration.getKafkaNamespace());
    }
}
