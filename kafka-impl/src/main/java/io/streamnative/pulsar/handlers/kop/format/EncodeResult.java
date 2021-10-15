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
import io.netty.util.Recycler;
import lombok.Getter;
import org.apache.kafka.common.record.MemoryRecords;

/**
 * Result of encode in entry formatter.
 */
@Getter
public class EncodeResult {

    private MemoryRecords records;
    private ByteBuf encodedByteBuf;
    private int numMessages;

    private final Recycler.Handle<EncodeResult> recyclerHandle;

    public static EncodeResult get(MemoryRecords records,
                                   ByteBuf encodedByteBuf,
                                   int numMessages) {
        EncodeResult encodeResult = RECYCLER.get();
        encodeResult.records = records;
        encodeResult.encodedByteBuf = encodedByteBuf;
        encodeResult.numMessages = numMessages;
        return encodeResult;
    }

    private EncodeResult(Recycler.Handle<EncodeResult> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<EncodeResult> RECYCLER = new Recycler<EncodeResult>() {
        @Override
        protected EncodeResult newObject(Recycler.Handle<EncodeResult> handle) {
            return new EncodeResult(handle);
        }
    };

    public void recycle() {
        records = null;
        if (encodedByteBuf != null) {
            encodedByteBuf.release();
            encodedByteBuf = null;
        }
        numMessages = -1;
        recyclerHandle.recycle(this);
    }

}
