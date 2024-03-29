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

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * A <i>Prometheus</i> based {@link PrometheusRawMetricsProvider} implementation.
 */
public class PrometheusMetricsProvider implements PrometheusRawMetricsProvider {
    private ScheduledExecutorService executor;

    public static final String PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = "prometheusStatsLatencyRolloverSeconds";
    public static final int DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = 60;

    public static final String PROMETHEUS_STATS_EXPIRED_SECONDS = "prometheusStatsExpiredSeconds";

    public static final long DEFAULT_PROMETHEUS_STATS_EXPIRED_SECONDS = TimeUnit.HOURS.toSeconds(1);

    private long expiredTimeSeconds;

    private static final String KOP_PROMETHEUS_STATS_CLUSTER = "cluster";
    private final Map<String, String> defaultStatsLoggerLabels = new HashMap<>();

    private final CollectorRegistry registry;

    /*
     * These acts a registry of the metrics defined in this provider
     */
    public final ConcurrentMap<ScopeContext, LongAdderCounter> counters = new ConcurrentHashMap<>();
    public final ConcurrentMap<ScopeContext, SimpleGauge<? extends Number>> gauges = new ConcurrentHashMap<>();
    public final ConcurrentMap<ScopeContext, DataSketchesOpStatsLogger> opStats = new ConcurrentHashMap<>();

    public PrometheusMetricsProvider() {
        this(CollectorRegistry.defaultRegistry);
    }

    public PrometheusMetricsProvider(CollectorRegistry registry) {
        this.registry = registry;
    }

    public void start(Configuration conf) {

        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("metrics"));

        int latencyRolloverSeconds = conf.getInt(PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS);

        expiredTimeSeconds = conf.getLong(PROMETHEUS_STATS_EXPIRED_SECONDS,
                DEFAULT_PROMETHEUS_STATS_EXPIRED_SECONDS);

        defaultStatsLoggerLabels.putIfAbsent(KOP_PROMETHEUS_STATS_CLUSTER,
                conf.getString(KOP_PROMETHEUS_STATS_CLUSTER));

        executor.scheduleAtFixedRate(() -> {
            rotateLatencyCollectionAndExpire(expiredTimeSeconds);
        }, 1, latencyRolloverSeconds, TimeUnit.SECONDS);

    }

    public void stop() {
        executor.shutdown();
    }

    public StatsLogger getStatsLogger(String scope) {
        return new PrometheusStatsLogger(PrometheusMetricsProvider.this, scope, defaultStatsLoggerLabels);
    }

    @Override
    public void generate(SimpleTextOutputStream writer) {
        gauges.forEach((sc, gauge) -> PrometheusTextFormatUtil.writeGauge(writer, sc.getScope(), gauge));
        counters.forEach((sc, counter) -> PrometheusTextFormatUtil.writeCounter(writer, sc.getScope(), counter));
        opStats.forEach((sc, opStatLogger) ->
                PrometheusTextFormatUtil.writeOpStat(writer, sc.getScope(), opStatLogger));
    }

    public String getStatsName(String... statsComponents) {
        String completeName;
        if (statsComponents.length == 0) {
            return "";
        } else if (statsComponents[0].isEmpty()) {
            completeName = StringUtils.join(statsComponents, '_', 1, statsComponents.length);
        } else {
            completeName = StringUtils.join(statsComponents, '_');
        }
        return Collector.sanitizeMetricName(completeName);
    }

    @VisibleForTesting
    void rotateLatencyCollectionAndExpire(long expiredTimeSeconds) {
        opStats.forEach((name, metric) -> {
            metric.rotateLatencyCollection(expiredTimeSeconds);
        });
    }
}
