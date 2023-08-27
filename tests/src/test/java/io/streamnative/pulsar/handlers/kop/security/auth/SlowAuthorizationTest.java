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
package io.streamnative.pulsar.handlers.kop.security.auth;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class SlowAuthorizationTest extends KafkaAuthorizationMockTestBase {

    @BeforeClass
    public void setup() throws Exception {
        super.authorizationProviderClassName = SlowMockAuthorizationProvider.class.getName();
        super.setup();
    }

    @AfterClass
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test(timeOut = 60000)
    public void testManyMessages() throws Exception {
        String superUserToken = AuthTokenUtils.createToken(SECRET_KEY, "normal-user", Optional.empty());
        final String topic = "test-many-messages";
        @Cleanup
        final KProducer kProducer = new KProducer(topic, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE, "token:" + superUserToken);
        long start = System.currentTimeMillis();
        log.info("Before send");
        for (int i = 0; i < 1000; i++) {
            kProducer.getProducer().send(new ProducerRecord(topic, "msg-" + i)).get();
        }
        log.info("After send ({} ms)", System.currentTimeMillis() - start);
        @Cleanup
        KConsumer kConsumer = new KConsumer(topic, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + superUserToken, "DemoKafkaOnPulsarConsumer");
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));
        int i = 0;
        start = System.currentTimeMillis();
        log.info("Before poll");
        while (i < 1000) {
            final ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            i += records.count();
        }
        log.info("After poll ({} ms)", System.currentTimeMillis() - start);
    }

    public static class SlowMockAuthorizationProvider extends KafkaMockAuthorizationProvider {

        @Override
        public CompletableFuture<Boolean> isSuperUser(String role, ServiceConfiguration serviceConfiguration) {
            return CompletableFuture.completedFuture(role.equals("pass.pass"));
        }

        @Override
        public CompletableFuture<Boolean> isSuperUser(String role, AuthenticationDataSource authenticationData,
                                                      ServiceConfiguration serviceConfiguration) {
            return CompletableFuture.completedFuture(role.equals("pass.pass"));
        }

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                          AuthenticationDataSource authenticationData) {
            return authorizeSlowly();
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(
                TopicName topicName, String role, AuthenticationDataSource authenticationData, String subscription) {
            return authorizeSlowly();
        }

        private static CompletableFuture<Boolean> authorizeSlowly() {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return CompletableFuture.completedFuture(true);
        }

        @Override
        CompletableFuture<Boolean> roleAuthorizedAsync(String role) {
            return CompletableFuture.completedFuture(true);
        }
    }
}
