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
import java.util.concurrent.atomic.AtomicLong;

public class DelayedFetch extends DelayedOperation {
    private final Runnable callback;
    private final AtomicLong bytesReadable;
    private final int minBytes;

    protected DelayedFetch(long delayMs, AtomicLong bytesReadable, int minBytes, Runnable callback) {
        super(delayMs, Optional.empty());
        this.callback = callback;
        this.bytesReadable = bytesReadable;
        this.minBytes = minBytes;
    }

    @Override
    public void onExpiration() {
        callback.run();
    }

    @Override
    public void onComplete() {
        callback.run();
    }

    @Override
    public boolean tryComplete() {
        if (bytesReadable.get() < minBytes){
            return false;
        }
        callback.run();
        return true;
    }
}
