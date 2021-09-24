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


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Validate CacheInvalidator.
 */
@Slf4j
public class CacheInvalidatorTest extends KopProtocolHandlerTestBase {


    @Test(timeOut = 20000, enabled = false)
    public void testCacheInvalidatorIsTriggered() throws Exception {
        String topic = "testCacheInvalidatorIsTriggered";
        @Cleanup
        KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());
        kProducer.getProducer().send(new ProducerRecord<>(topic, 1, "value"));

        @Cleanup
        KConsumer kConsumer = new KConsumer(topic, getKafkaBrokerPort(), true);
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));

        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(5));
        assertNotNull(records);
        assertEquals(1, records.count());
        ConsumerRecord<Integer, String> record = records.iterator().next();
        assertEquals(1, record.key().intValue());
        assertEquals("value", record.value());

        log.info("LOOKUP_CACHE {}", KopBrokerLookupManager.LOOKUP_CACHE);
        log.info("KOP_ADDRESS_CACHE {}", KopBrokerLookupManager.KOP_ADDRESS_CACHE);

        assertFalse(KopBrokerLookupManager.LOOKUP_CACHE.isEmpty());
        assertFalse(KopBrokerLookupManager.KOP_ADDRESS_CACHE.isEmpty());

        pulsar.getAdminClient().topics().unload(topic);

        Awaitility.await().untilAsserted(() -> {
            log.info("LOOKUP_CACHE {}", KopBrokerLookupManager.LOOKUP_CACHE);
            log.info("KOP_ADDRESS_CACHE {}", KopBrokerLookupManager.KOP_ADDRESS_CACHE);
            assertTrue(KopBrokerLookupManager.LOOKUP_CACHE.isEmpty());
            assertTrue(KopBrokerLookupManager.KOP_ADDRESS_CACHE.isEmpty());
        });

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
