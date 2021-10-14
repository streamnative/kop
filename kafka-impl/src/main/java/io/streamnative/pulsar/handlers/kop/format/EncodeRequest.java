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

import io.netty.util.Recycler;
import org.apache.kafka.common.record.MemoryRecords;

/**
 * Request of encode in entry formatter.
 */
public class EncodeRequest {

    MemoryRecords records;
    long baseOffset;

    private final Recycler.Handle<EncodeRequest> recyclerHandle;

    public static EncodeRequest get(MemoryRecords records) {
        return get(records, 0L);
    }

    public static EncodeRequest get(MemoryRecords records,
                                    long baseOffset) {
        EncodeRequest encodeRequest = RECYCLER.get();
        encodeRequest.records = records;
        encodeRequest.baseOffset = baseOffset;
        return encodeRequest;
    }

    private EncodeRequest(Recycler.Handle<EncodeRequest> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<EncodeRequest> RECYCLER = new Recycler<EncodeRequest>() {
        @Override
        protected EncodeRequest newObject(Recycler.Handle<EncodeRequest> handle) {
            return new EncodeRequest(handle);
        }
    };

    public void recycle() {
        records = null;
        baseOffset = -1L;
        recyclerHandle.recycle(this);
    }

    public void setBaseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public MemoryRecords getRecords() {
        return records;
    }

    public long getBaseOffset() {
        return baseOffset;
    }
}
