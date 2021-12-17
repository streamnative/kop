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
import io.streamnative.pulsar.handlers.kop.storage.PartitionLog;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.record.MemoryRecords;

/**
 * Request of encode in entry formatter.
 */
@Getter
public class EncodeRequest {

    private MemoryRecords records;
    @Setter
    private PartitionLog.LogAppendInfo appendInfo;

    private final Recycler.Handle<EncodeRequest> recyclerHandle;

    public static EncodeRequest get(MemoryRecords records,
                                    PartitionLog.LogAppendInfo appendInfo) {
        EncodeRequest encodeRequest = RECYCLER.get();
        encodeRequest.records = records;
        encodeRequest.appendInfo = appendInfo;
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
        appendInfo = null;
        recyclerHandle.recycle(this);
    }

}
