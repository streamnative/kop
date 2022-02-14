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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;

/**
 * InternalServerCnx, this only used to construct internalProducer / internalConsumer.
 * So when topic is unload, we could disconnect the connection between kafkaRequestHandler and client,
 * by internalProducer / internalConsumer.close();
 * which means when topic unload happens, we should close the connection.
 */
@Slf4j
public class InternalProducer extends Producer {
    private ServerCnx serverCnx;
    public InternalProducer(Topic topic, ServerCnx cnx,
                            long producerId, String producerName) {
        super(topic, cnx, producerId, producerName, null,
                false, null, null, 0, false,
                ProducerAccessMode.Shared, Optional.empty(), false);
        this.serverCnx = cnx;
    }

    // this will call back by bundle unload
    @Override
    public CompletableFuture<Void> disconnect() {
        InternalServerCnx cnx = (InternalServerCnx) getCnx();
        CompletableFuture<Void> future = new CompletableFuture<>();

        cnx.getBrokerService().executor().execute(() -> {
            log.info("Disconnecting producer: {}", this);
            getTopic().removeProducer(this);
            cnx.closeProducer(this);
            future.complete(null);
        });

        return future;
    }

    @Override
    public ServerCnx getCnx() {
        return serverCnx;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
