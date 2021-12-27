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

import lombok.AllArgsConstructor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

@AllArgsConstructor
public class KafkaPositionImpl implements Position {

    protected long offset;
    protected long ledgerId;
    protected long entryId;

    public static final KafkaPositionImpl EARLIEST = new KafkaPositionImpl(0, -1, -1);
    public static final KafkaPositionImpl LATEST =
            new KafkaPositionImpl(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);

    public static KafkaPositionImpl get(long offset, long ledgerId, long entryId) {
        return new KafkaPositionImpl(offset, ledgerId, entryId);
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public Position getNext() {
        if (entryId < 0) {
            return PositionImpl.get(ledgerId, 0);
        } else {
            return PositionImpl.get(ledgerId, entryId + 1);
        }
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }
}
