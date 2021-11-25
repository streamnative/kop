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

import java.util.function.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.Schema;

/**
 * Process Kafka messages so that Pulsar consumer can recognize.
 */
public class KafkaPayloadProcessor implements MessagePayloadProcessor {

    @Override
    public <T> void process(MessagePayload payload,
                            MessagePayloadContext context,
                            Schema<T> schema,
                            Consumer<Message<T>> messageConsumer) throws Exception {
        // TODO: convert Kafka messages
        DEFAULT.process(payload, context, schema, messageConsumer);
    }
}
