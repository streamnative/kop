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
package io.streamnative.pulsar.handlers.kop;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Slf4j
public class KopBrokerLookupManagerTest extends KopProtocolHandlerTestBase {

    private static final String TENANT = "test";
    private static final String NAMESPACE = TENANT + "/" + "kop-broker-lookup-manager-test";

    private LookupClient lookupClient;
    private KopBrokerLookupManager kopBrokerLookupManager;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");
        admin.tenants().createTenant(TENANT, TenantInfo.builder()
                .adminRoles(Collections.emptySet())
                .allowedClusters(Collections.singleton(configClusterName))
                .build());
        admin.namespaces().createNamespace(NAMESPACE);
        admin.namespaces().setDeduplicationStatus(NAMESPACE, true);
        lookupClient = new LookupClient(pulsar, conf);
        kopBrokerLookupManager = new KopBrokerLookupManager(conf, pulsar, lookupClient);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        kopBrokerLookupManager.close();
        lookupClient.close();
    }

    @Test(timeOut = 20 * 1000)
    public void testIsTopicExists() throws Exception {
        String existsTopic = "persistent://" + NAMESPACE + "/" + "exists-topic";
        String nonExistentTopic = "persistent://" + NAMESPACE + "/" + "non-existent-topic";
        String nonPartitionedExistsTopic = "persistent://" + NAMESPACE + "/" + "non-partitioned-exists-topic";
        admin.topics().createPartitionedTopic(existsTopic, 2);
        assertTrue(kopBrokerLookupManager.isTopicExists(existsTopic).get());
        assertFalse(kopBrokerLookupManager.isTopicExists(nonExistentTopic).get());

        admin.topics().createNonPartitionedTopicAsync(nonPartitionedExistsTopic);
        assertTrue(kopBrokerLookupManager.isTopicExists(existsTopic).get());
    }

}
