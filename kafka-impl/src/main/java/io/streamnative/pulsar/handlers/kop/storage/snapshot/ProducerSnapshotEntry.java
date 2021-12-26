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
package io.streamnative.pulsar.handlers.kop.storage.snapshot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ProducerSnapshotEntry {
    /**
     * The producer ID
     */
    private long producerId;

    /**
     * Current epoch of the producer
     */
    private short producerEpoch;

    /**
     * Last written sequence of the producer
     */
    private int lastSequence;

    /**
     * Last written offset of the producer
     */
    private long lastOffset;

    /**
     * The difference of the last sequence and first sequence in the last written batch
     */
    private int offsetDelta;

    /**
     * Max timestamp from the last written entry
     */
    private long timestamp;

    /**
     * The epoch of the last transaction coordinator to send an end transaction marker
     */
    private int coordinatorEpoch;

    /**
     * The first offset of the on-going transaction (-1 if there is none)
     */
    private long currentTxnFirstOffset;
}
