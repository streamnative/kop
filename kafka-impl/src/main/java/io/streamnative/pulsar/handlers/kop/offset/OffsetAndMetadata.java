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
package io.streamnative.pulsar.handlers.kop.offset;

import static org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP;

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Offset and metadata.
 */
@Data
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class OffsetAndMetadata {

    public static final String NoMetadata = "";

    private final OffsetMetadata offsetMetadata;
    private final long commitTimestamp;
    private final long expireTimestamp;

    public static OffsetAndMetadata apply(
        long offset,
        String metadata,
        long commitTimestamp,
        long expireTimestamp
    ) {
        return new OffsetAndMetadata(
            new OffsetMetadata(offset, metadata),
            commitTimestamp,
            expireTimestamp
        );
    }

    public static OffsetAndMetadata apply(
        long offset,
        String metadata,
        long timestamp
    ) {
        return new OffsetAndMetadata(
            new OffsetMetadata(offset, metadata),
            timestamp,
            timestamp
        );
    }

    public static OffsetAndMetadata apply(
        long offset,
        String metadata
    ) {
        return new OffsetAndMetadata(
            new OffsetMetadata(offset, metadata)
        );
    }

    public static OffsetAndMetadata apply(
        long offset
    ) {
        return new OffsetAndMetadata(
            new OffsetMetadata(offset, OffsetMetadata.NO_METADATA)
        );
    }

    @SuppressWarnings("deprecation")
    private OffsetAndMetadata(OffsetMetadata offsetMetadata) {
        this(
            offsetMetadata,
            DEFAULT_TIMESTAMP,
            DEFAULT_TIMESTAMP);
    }

    public long offset() {
        return offsetMetadata.offset();
    }

    public String metadata() {
        return offsetMetadata.metadata();
    }

    @Override
    public String toString() {
        return String.format(
            "[%s,CommitTime %d,ExpirationTime %d]",
            offsetMetadata,
            commitTimestamp,
            expireTimestamp
        );
    }

}
