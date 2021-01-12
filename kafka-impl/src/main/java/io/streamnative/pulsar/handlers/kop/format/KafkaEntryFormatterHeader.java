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
package io.streamnative.pulsar.handlers.kop.format;

import org.apache.pulsar.common.api.proto.PulsarApi;


/**
 * The header of KafkaEntryFormatter.
 */
public class KafkaEntryFormatterHeader {

    public PulsarApi.MessageMetadata getMessageMetadataWithNumberMessages(int numMessages) {
        final PulsarApi.MessageMetadata.Builder builder = PulsarApi.MessageMetadata.newBuilder();
        builder.addProperties(PulsarApi.KeyValue.newBuilder()
                .setKey("entry.format")
                .setValue(EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase())
                .build());
        builder.setProducerName("");
        builder.setSequenceId(0L);
        builder.setPublishTime(0L);
        builder.setNumMessagesInBatch(numMessages);
        return builder.build();
    }

}
