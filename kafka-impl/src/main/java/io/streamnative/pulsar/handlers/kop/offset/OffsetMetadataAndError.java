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

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.protocol.Errors;

/**
 * Offset metadata and errors.
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class OffsetMetadataAndError {

    public static final OffsetMetadataAndError NO_OFFSET =
        new OffsetMetadataAndError(OffsetMetadata.INVALID_OFFSET_METADATA, Errors.NONE);
    public static final OffsetMetadataAndError GROUP_LOADING =
        new OffsetMetadataAndError(OffsetMetadata.INVALID_OFFSET_METADATA, Errors.COORDINATOR_LOAD_IN_PROGRESS);
    public static final OffsetMetadataAndError UNKNOWN_MEMBER =
        new OffsetMetadataAndError(OffsetMetadata.INVALID_OFFSET_METADATA, Errors.UNKNOWN_MEMBER_ID);
    public static final OffsetMetadataAndError NOT_COORDINATOR_FOR_GROUP =
        new OffsetMetadataAndError(OffsetMetadata.INVALID_OFFSET_METADATA, Errors.NOT_COORDINATOR);
    public static final OffsetMetadataAndError GROUP_COORDINATOR_NOT_AVAILABLE =
        new OffsetMetadataAndError(OffsetMetadata.INVALID_OFFSET_METADATA, Errors.COORDINATOR_NOT_AVAILABLE);
    public static final OffsetMetadataAndError UNKNOWN_TOPIC_OR_PARTITION =
        new OffsetMetadataAndError(OffsetMetadata.INVALID_OFFSET_METADATA, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    public static final OffsetMetadataAndError ILLEGAL_GROUP_GENERATION_ID =
        new OffsetMetadataAndError(OffsetMetadata.INVALID_OFFSET_METADATA, Errors.ILLEGAL_GENERATION);

    public static OffsetMetadataAndError apply(long offset) {
        return new OffsetMetadataAndError(
            new OffsetMetadata(offset, OffsetMetadata.NO_METADATA),
            Errors.NONE
        );
    }

    public static OffsetMetadataAndError apply(Errors errors) {
        return new OffsetMetadataAndError(
            OffsetMetadata.INVALID_OFFSET_METADATA,
            errors
        );
    }

    public static OffsetMetadataAndError apply(long offset,
                                               String metadata,
                                               Errors errors) {
        return new OffsetMetadataAndError(
            new OffsetMetadata(offset, metadata),
            errors
        );
    }

    private final OffsetMetadata offsetMetadata;
    private final Errors error;

    private OffsetMetadataAndError(OffsetMetadata offsetMetadata) {
        this(offsetMetadata, Errors.NONE);
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
            "[%s, Error=%s]",
            offsetMetadata,
            error
        );
    }
}
