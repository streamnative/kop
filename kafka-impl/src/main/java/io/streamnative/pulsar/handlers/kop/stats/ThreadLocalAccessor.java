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

import com.yahoo.sketches.quantiles.DoublesUnion;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ThreadLocalAccessor {

    private final Map<LocalData, Boolean> map = new ConcurrentHashMap<>();
    private final FastThreadLocal<LocalData> localData = new FastThreadLocal<LocalData>() {

        @Override
        protected LocalData initialValue() {
            LocalData localData = new LocalData();
            map.put(localData, Boolean.TRUE);
            return localData;
        }

        @Override
        protected void onRemoval(LocalData value) {
            map.remove(value);
        }
    };

    public void recordAndCheckStatsExpire(DoublesUnion aggregateSuccess, DoublesUnion aggregateFail, long expireTimeMs) {
        long currentTime = System.currentTimeMillis();

        map.keySet().forEach(key -> {
            // update stats
            key.record(aggregateSuccess, aggregateFail);

            // check if record expired.
            if (currentTime - key.lastHasRecordTime() > expireTimeMs) {
                map.remove(key);
            }
        });
    }

    public LocalData getLocalData() {
        return localData.get();
    }
}
