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
package io.streamnative.pulsar.handlers.kop.utils;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Predicate for find position for a given offset(index).
 */
public class OffsetSearchPredicate implements com.google.common.base.Predicate<Entry> {
    private static final Logger log = LoggerFactory.getLogger(OffsetSearchPredicate.class);

    long indexToSearch = -1;
    public OffsetSearchPredicate(long indexToSearch) {
        this.indexToSearch = indexToSearch;
    }

    @Override
    public boolean apply(@Nullable Entry entry) {
        try {
            BrokerEntryMetadata brokerEntryMetadata =
                    Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
            return brokerEntryMetadata.getIndex() < indexToSearch;
        } catch (Exception e) {
            log.error("Error deserialize message for message position find", e);
        } finally {
            entry.release();
        }
        return false;
    }
}