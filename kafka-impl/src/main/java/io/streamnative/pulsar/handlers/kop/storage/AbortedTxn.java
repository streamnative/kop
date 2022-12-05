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
package io.streamnative.pulsar.handlers.kop.storage;

import java.nio.ByteBuffer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * AbortedTxn is used cache the aborted index.
 */
@Data
@Accessors(fluent = true)
@AllArgsConstructor
public final class AbortedTxn {

    private static final int VersionOffset = 0;
    private static final int VersionSize = 2;
    private static final int ProducerIdOffset = VersionOffset + VersionSize;
    private static final int ProducerIdSize = 8;
    private static final int FirstOffsetOffset = ProducerIdOffset + ProducerIdSize;
    private static final int FirstOffsetSize = 8;
    private static final int LastOffsetOffset = FirstOffsetOffset + FirstOffsetSize;
    private static final int LastOffsetSize = 8;
    private static final int LastStableOffsetOffset = LastOffsetOffset + LastOffsetSize;
    private static final int LastStableOffsetSize = 8;
    private static final int TotalSize = LastStableOffsetOffset + LastStableOffsetSize;

    private static final Short CurrentVersion = 0;

    private final Long producerId;
    private final Long firstOffset;
    private final Long lastOffset;
    private final Long lastStableOffset;

    protected ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(AbortedTxn.TotalSize);
        buffer.putShort(CurrentVersion);
        buffer.putLong(producerId);
        buffer.putLong(firstOffset);
        buffer.putLong(lastOffset);
        buffer.putLong(lastStableOffset);
        buffer.flip();
        return buffer;
    }
}
