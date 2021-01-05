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

import java.util.LinkedList;
import java.util.Queue;

/**
 * Queue of PendingProduce instances.
 */
public class PendingProduceQueue {

    private final Queue<PendingProduce> queue = new LinkedList<>();

    public synchronized void sendCompletedProduces() {
        while (!queue.isEmpty()) {
            PendingProduce pendingProduce = queue.peek();
            if (!pendingProduce.ready()) {
                break;
            }
            queue.remove();
            pendingProduce.publishMessages();
        }
    }

    public synchronized void add(PendingProduce pendingProduce) {
        queue.add(pendingProduce);
    }
}
