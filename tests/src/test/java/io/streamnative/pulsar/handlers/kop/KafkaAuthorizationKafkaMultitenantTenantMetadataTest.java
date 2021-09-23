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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for Authorization with `KafkaEnableMultiTenantMetadata` = false.
 */
public class KafkaAuthorizationKafkaMultitenantTenantMetadataTest extends KafkaAuthorizationTestBase {
    public KafkaAuthorizationKafkaMultitenantTenantMetadataTest() {
        super("pulsar");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setKafkaEnableMultiTenantMetadata(false);
        super.setup();
    }

    @Test
    void testKafkaEnableMultiTenantMetadataIsDisabled() {
        // verify that the configuration is with KafkaEnableMultiTenantMetadata=false
        // and the setup did not reset the value to the default
        assertFalse(conf.isKafkaEnableMultiTenantMetadata());
    }
}
