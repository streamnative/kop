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

import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DelayedFetch extends DelayedOperation {
    private final Runnable callback;
    private final AtomicLong bytesReadable;
    private final int minBytes;
    private final MessageFetchContext messageFetchContext;
    private final AtomicBoolean restarted = new AtomicBoolean();
    private final AtomicBoolean someMessageProduced = new AtomicBoolean();

    protected DelayedFetch(long delayMs, AtomicLong bytesReadable, int minBytes,
                           MessageFetchContext messageFetchContext) {
        super(delayMs, Optional.empty());
        this.bytesReadable = bytesReadable;
        this.minBytes = minBytes;
        this.messageFetchContext = messageFetchContext;
        this.callback = messageFetchContext::complete;
    }

    @Override
    public void onExpiration() {
        if (restarted.get()) {
            return;
        }
        callback.run();
    }

    @Override
    public void onComplete() {
        if (restarted.get()) {
            return;
        }
        callback.run();
    }

    @Override
    public boolean tryComplete() {
        if (someMessageProduced.get()) {
            // if we are here then we were waiting for the condition
            // someone wrote some messages to one of the topics
            // trigger the Fetch from scratch
            restarted.set(true);
            messageFetchContext.onDataWrittenToSomePartition();
            return true;
        }
        if (bytesReadable.get() < minBytes){
            return false;
        }
        callback.run();
        return true;
    }

    @Override
    public boolean wakeup() {
        // In the future we could notify the MessageFetchContext that the
        // new data is only on this partition and not
        // on other partitions
        someMessageProduced.set(true);
        return true;
    }
}
