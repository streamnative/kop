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
package io.streamnative.pulsar.handlers.kop.migration.processor;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.http.HttpJsonRequestProcessor;
import io.streamnative.pulsar.handlers.kop.migration.metadata.MigrationMetadataManager;
import io.streamnative.pulsar.handlers.kop.migration.requests.CreateTopicWithMigrationRequest;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Http processor for creating a KoP topic with migration configuration.
 */
@Slf4j
public class CreateTopicWithMigrationProcessor
        extends HttpJsonRequestProcessor<CreateTopicWithMigrationRequest, String> {
    private final KafkaServiceConfiguration kafkaConfig;
    private final MigrationMetadataManager migrationMetadataManager;

    public CreateTopicWithMigrationProcessor(Class<CreateTopicWithMigrationRequest> requestModel,
                                             KafkaServiceConfiguration kafkaConfig,
                                             MigrationMetadataManager migrationMetadataManager) {
        super(requestModel, "/migration/createTopic", "POST");
        this.kafkaConfig = kafkaConfig;
        this.migrationMetadataManager = migrationMetadataManager;
    }

    @Override
    protected int statusCodeFromThrowable(Throwable err) {
        if (err instanceof UnknownTopicOrPartitionException) {
            return HttpResponseStatus.NOT_FOUND.code();
        }
        if (err instanceof ApiException) {
            if (err instanceof TopicExistsException) {
                return HttpResponseStatus.CONFLICT.code();
            }
            return HttpResponseStatus.BAD_REQUEST.code();
        }
        return HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
    }

    @Override
    protected CompletableFuture<String> processRequest(CreateTopicWithMigrationRequest payload,
                                                       List<String> patternGroups, FullHttpRequest request,
                                                       Channel channel) {
        String topic = payload.getTopic();
        return migrationMetadataManager.createWithMigration(topic, "public/default", payload.getKafkaClusterAddress(),
                        channel)
                .thenApply(ignored -> "Topic " + topic + " created");
    }
}
