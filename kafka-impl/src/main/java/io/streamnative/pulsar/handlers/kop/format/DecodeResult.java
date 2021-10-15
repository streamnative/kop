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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.common.record.MemoryRecords;

/**
 * Result of decode in entry formatter.
 */
public class DecodeResult {

    @Getter
    private MemoryRecords records;
    private ByteBuf releasedByteBuf;

    private final Recycler.Handle<DecodeResult> recyclerHandle;

    public static DecodeResult get(MemoryRecords records) {
        return get(records, null);
    }

    public static DecodeResult get(MemoryRecords records,
                                   ByteBuf releasedByteBuf) {
        DecodeResult decodeResult = RECYCLER.get();
        decodeResult.records = records;
        decodeResult.releasedByteBuf = releasedByteBuf;
        return decodeResult;
    }

    private DecodeResult(Recycler.Handle<DecodeResult> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<DecodeResult> RECYCLER = new Recycler<DecodeResult>() {
        @Override
        protected DecodeResult newObject(Recycler.Handle<DecodeResult> handle) {
            return new DecodeResult(handle);
        }
    };

    public void recycle() {
        records = null;
        if (releasedByteBuf != null) {
            releasedByteBuf.release();
            releasedByteBuf = null;
        }
        recyclerHandle.recycle(this);
    }

    public @NonNull ByteBuf getOrCreateByteBuf() {
        if (releasedByteBuf != null) {
            return releasedByteBuf;
        } else {
            return Unpooled.wrappedBuffer(records.buffer());
        }
    }

}
