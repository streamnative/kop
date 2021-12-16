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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A delayed create partitions operation that is stored in the topic purgatory.
 */
class DelayedCreatePartitions extends DelayedOperation {

    private final AtomicInteger numTopics;
    private final Runnable callback;

    DelayedCreatePartitions(long delayMs, AtomicInteger numTopics, Runnable callback) {
        super(delayMs, Optional.empty());
        this.numTopics = numTopics;
        this.callback = callback;
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
    public boolean tryComplete(boolean notify) {
        if (numTopics.get() <= 0) {
            forceComplete();
            return true;
        } else {
            return false;
        }
    }
}
