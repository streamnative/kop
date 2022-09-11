/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.migration.processor;

import io.netty.handler.codec.http.FullHttpRequest;
import io.streamnative.pulsar.handlers.kop.AdminManager;
import io.streamnative.pulsar.handlers.kop.http.HttpJsonRequestProcessor;
import io.streamnative.pulsar.handlers.kop.migration.requests.CreateTopicWithMigrationRequest;
import io.streamnative.pulsar.handlers.kop.migration.workflow.MigrationWorkflowManager;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * Http processor for creating a KoP topic with migration configuration.
 */
@Slf4j
public class CreateTopicWithMigrationProcessor
        extends HttpJsonRequestProcessor<CreateTopicWithMigrationRequest, String> {
    private final AdminManager adminManager;
    private final BrokerService brokerService;
    private final MigrationWorkflowManager migrationWorkflowManager;

    public CreateTopicWithMigrationProcessor(Class<CreateTopicWithMigrationRequest> requestModel,
                                             AdminManager adminManager,
                                             MigrationWorkflowManager migrationWorkflowManager) {
        super(requestModel, "/migration/createTopic", "POST");
        this.adminManager = adminManager;
        this.migrationWorkflowManager = migrationWorkflowManager;
    }

    @Override
    protected CompletableFuture<String> processRequest(CreateTopicWithMigrationRequest payload,
                                                       List<String> patternGroups,
                                                       FullHttpRequest request) {
        String topic = payload.getTopic();
//        Properties properties = new Properties();
//        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, payload.getKafkaClusterAddress());
//        AdminClient kafkaAdminClient = AdminClient.create(properties);

        // TODO: Check existence in Kafka
//        try {
//            log.info("kafkaAdminClient.describeTopics: " + kafkaAdminClient.describeTopics(Collections
//            .singletonList(payload.getTopic())));
//        } catch (UnknownTopicOrPartitionException e) {
//            throw new IllegalArgumentException("Topic doesn't exist in Kafka");
//        }

        return migrationWorkflowManager.createWithMigration(payload.getTopic(), payload.getKafkaClusterAddress())
                .thenApply(ignored -> "Topic " + topic + " created");
    }
}
