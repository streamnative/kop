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

import static io.streamnative.pulsar.handlers.kop.KopServerStats.CATEGORY_SERVER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.KOP_EVENT_QUEUE_SIZE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;

import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.annotations.StatsDoc;


/**
 * Kop event manager stats for prometheus metrics.
 */
@StatsDoc(
        name = SERVER_SCOPE,
        category = CATEGORY_SERVER,
        help = "KOP event manager stats"
)

@Getter
@Slf4j
public class KopEventManagerStats {

    public static final AtomicInteger KOP_EVENT_QUEUE_SIZE_INSTANCE = new AtomicInteger(0);


    private final StatsLogger statsLogger;

    public KopEventManagerStats(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        statsLogger.registerGauge(KOP_EVENT_QUEUE_SIZE, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return KOP_EVENT_QUEUE_SIZE_INSTANCE;
            }
        });
    }

}
