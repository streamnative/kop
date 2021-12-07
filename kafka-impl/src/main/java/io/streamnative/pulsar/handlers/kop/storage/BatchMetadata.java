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

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * BatchMetadata is used to check the message duplicate.
 */
@Getter
@AllArgsConstructor
public class BatchMetadata {

    private final Integer lastSeq;
    private final Long lastOffset;
    // Should be seq delta, we use offsetDelta here because in future might change back.
    // When we preset the correct offset before message publish.
    private final Integer offsetDelta;
    private final Long timestamp;

    public int firstSeq() {
        return decrementSequence(lastSeq, offsetDelta);
    }

    public Long firstOffset() {
        return lastOffset - offsetDelta;
    }

    private int decrementSequence(int sequence, int decrement) {
        if (sequence < decrement) {
            return Integer.MAX_VALUE - (decrement - sequence) + 1;
        }
        return sequence - decrement;
    }

    @Override
    public String toString() {
        return "BatchMetadata("
                + "firstSeq=" + firstSeq() + ", "
                + "lastSeq=" + lastSeq + ", "
                + "firstOffset=" + firstOffset() + ", "
                + "lastOffset=" + lastOffset + ", "
                + "timestamp=" + timestamp + ")";
    }
}

