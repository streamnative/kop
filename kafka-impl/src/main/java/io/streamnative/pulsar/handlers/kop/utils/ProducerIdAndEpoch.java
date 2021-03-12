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

import org.apache.kafka.common.record.RecordBatch;

/**
 * ProducerId and epoch.
 */
public class ProducerIdAndEpoch {
    public static final ProducerIdAndEpoch NONE =
            new ProducerIdAndEpoch(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH);

    public final long producerId;
    public final short epoch;

    public ProducerIdAndEpoch(long producerId, short epoch) {
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public boolean isValid() {
        return RecordBatch.NO_PRODUCER_ID < producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return "(producerId=" + producerId + ", epoch=" + epoch + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProducerIdAndEpoch that = (ProducerIdAndEpoch) o;

        if (producerId != that.producerId) {
            return false;
        }
        return epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        int result = (int) (producerId ^ (producerId >>> 32));
        result = 31 * result + (int) epoch;
        return result;
    }

}
