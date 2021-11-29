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

import io.netty.util.Recycler;
import lombok.Data;

/**
 * AppendInfoContext handling duplicate sequence number.
 */
@Data
public class AppendInfoContext {

    private final Recycler.Handle<AppendInfoContext> recyclerHandle;
    private PartitionLog kopLog;
    private ProducerStateManager.ProducerAppendInfo appendInfo;
    private Long producerId;
    private boolean isTransaction;

    private static final Recycler<AppendInfoContext> RECYCLER = new Recycler<AppendInfoContext>() {
        @Override
        protected AppendInfoContext newObject(Handle<AppendInfoContext> handle) {
            return new AppendInfoContext(handle);
        }
    };

    private AppendInfoContext(Recycler.Handle<AppendInfoContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static AppendInfoContext get(PartitionLog kopLog,
                                        ProducerStateManager.ProducerAppendInfo appendInfo,
                                        Long producerId,
                                        boolean isTransaction) {
        AppendInfoContext context = RECYCLER.get();
        context.kopLog = kopLog;
        context.appendInfo = appendInfo;
        context.producerId = producerId;
        context.isTransaction = isTransaction;
        return context;
    }

    private void recycle() {
        kopLog = null;
        appendInfo = null;
        producerId = null;
        isTransaction = false;
    }

    public void resetAppendInfoOffset(long offset) {
        if (appendInfo != null) {
            appendInfo.resetOffset(offset, isTransaction);
            kopLog.update(appendInfo);
        }
        recycle();
    }
}
