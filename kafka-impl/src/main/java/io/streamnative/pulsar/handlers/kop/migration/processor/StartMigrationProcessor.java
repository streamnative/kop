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

import io.netty.handler.codec.http.FullHttpRequest;
import io.streamnative.pulsar.handlers.kop.http.HttpJsonRequestProcessor;
import io.streamnative.pulsar.handlers.kop.migration.requests.StartMigrationRequest;
import io.streamnative.pulsar.handlers.kop.migration.workflow.MigrationWorkflowManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Http processor for starting a Kafka to KoP migration.
 */
@Slf4j
public class StartMigrationProcessor extends HttpJsonRequestProcessor<StartMigrationRequest, Void> {
    private final MigrationWorkflowManager migrationWorkflowManager;
    public StartMigrationProcessor(Class<StartMigrationRequest> requestModel,
                                   MigrationWorkflowManager migrationWorkflowManager) {
        super(requestModel, "/migration/start", "POST");
        this.migrationWorkflowManager = migrationWorkflowManager;
    }

    @Override
    protected CompletableFuture<Void> processRequest(StartMigrationRequest payload, List<String> patternGroups,
                                                     FullHttpRequest request) {
        migrationWorkflowManager.startMigration(payload.getTopic());
        return CompletableFuture.completedFuture(null);
    }
}
