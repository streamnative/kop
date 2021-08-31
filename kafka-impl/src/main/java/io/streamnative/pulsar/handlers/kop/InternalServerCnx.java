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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;

/**
 * InternalServerCnx, this only used to construct internalProducer / internalConsumer.
 * So when topic is unload, we could disconnect the connection between kafkaRequestHandler and client,
 * by internalProducer / internalConsumer.close();
 * which means when topic unload happens, we should close the connection.
 */
@Slf4j
public class InternalServerCnx extends ServerCnx {

    public static final SocketAddress MOCKED_REMOTE_ADDRESS = new InetSocketAddress("localhost", 9999);

    @Getter
    KafkaRequestHandler kafkaRequestHandler;

    public InternalServerCnx(KafkaRequestHandler kafkaRequestHandler) {
        super(kafkaRequestHandler.getPulsarService());
        this.kafkaRequestHandler = kafkaRequestHandler;
        // this is the client address that connect to this server.
        this.remoteAddress = kafkaRequestHandler.getRemoteAddress();

        // mock some values, or Producer create will meet NPE.
        // used in test, which will not call channel.active, and not call updateCtx.
        if (this.remoteAddress == null) {
            this.remoteAddress = MOCKED_REMOTE_ADDRESS;
        }
    }

    // this will call back by bundle unload
    @Override
    public void closeProducer(Producer producer) {
        // removes producer-connection from map and send close command to producer
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed topic: {}'s producer: {}.",
                remoteAddress, producer.getTopic().getName(), producer);
        }

        kafkaRequestHandler.close();
    }

    // called after channel active
    public void updateCtx(final SocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void enableCnxAutoRead() {
        // do nothing is this mock
    }

    @Override
    public void disableCnxAutoRead() {
        // do nothing is this mock
    }

    @Override
    public void cancelPublishBufferLimiting() {
        // do nothing is this mock
    }
}
