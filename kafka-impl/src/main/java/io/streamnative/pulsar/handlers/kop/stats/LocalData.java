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
package io.streamnative.pulsar.handlers.kop.stats;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import java.util.concurrent.locks.StampedLock;

public class LocalData {

    private final DoublesSketch successSketch = new DoublesSketchBuilder().build();
    private final DoublesSketch failSketch = new DoublesSketchBuilder().build();
    private final StampedLock lock = new StampedLock();

    public void updateSuccessSketch(double value) {
        long stamp = lock.readLock();
        try {
            successSketch.update(value);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public void updateFailedSketch(double value) {
        long stamp = lock.readLock();
        try {
            failSketch.update(value);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public void record(DoublesUnion aggregateSuccess, DoublesUnion aggregateFail) {
        long stamp = lock.writeLock();
        try {
            aggregateSuccess.update(successSketch);
            successSketch.reset();
            aggregateFail.update(failSketch);
            failSketch.reset();
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
