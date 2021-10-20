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
package io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl;

import static org.testng.Assert.assertEquals;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarSchemaStorageTest extends KopProtocolHandlerTestBase {


    @Test
    public void testUsingSchemaStorageTestsBase() throws Exception {
        SchemaStorageTestsBase tester = new SchemaStorageTestsBase(new SchemaStorageAccessor() {
            @Override
            public SchemaStorage getSchemaStorageForTenant(String tenant) throws SchemaStorageException {
                return new PulsarSchemaStorage(tenant, pulsarClient,
                        "persistent://public/default/__schemaregistry");
            }

            @Override
            public void close() {
                // nothing to do
            }
        });
        try {
            tester.testWriteGet();
        } finally {
            tester.stopAll();
        }
    }

    @Test
    public void testTwoInstances() throws Exception {
        String subject1 = "cccc";
        try (PulsarSchemaStorage instance1 = new PulsarSchemaStorage(tenant, pulsarClient,
                "persistent://public/default/__schemaregistry-2");
            PulsarSchemaStorage instance2 = new PulsarSchemaStorage(tenant, pulsarClient,
                    "persistent://public/default/__schemaregistry-2");) {
            // writing using instance1
            Schema schemaVersion = instance1.createSchemaVersion(subject1,
                    Schema.TYPE_AVRO, "{test}", true).get();

            // read using instance2
            Schema lookup2 = instance2.findSchemaById(schemaVersion.getId()).get();
            assertEquals(schemaVersion, lookup2);

            // read using instance1
            Schema lookup1 = instance2.findSchemaById(schemaVersion.getId()).get();
            assertEquals(schemaVersion, lookup1);
        }
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

}
