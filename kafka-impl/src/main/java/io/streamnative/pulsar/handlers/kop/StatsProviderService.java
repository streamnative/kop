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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

@Slf4j
public class StatsProviderService {
    private final StatsProvider statsProvider;
    private final Configuration conf;

    public StatsProviderService(KafkaServiceConfiguration kafkaServiceConfiguration) throws Exception {
        Class statsProviderClass = Class.forName(kafkaServiceConfiguration.getKopStatsProviderClass());
        this.statsProvider = (StatsProvider) statsProviderClass.newInstance();

        conf = new PropertiesConfiguration();
        conf.addProperty("prometheusStatsHttpEnable", kafkaServiceConfiguration.isKopPrometheusStatsHttpEnable());
        conf.addProperty("prometheusStatsHttpPort", kafkaServiceConfiguration.getKopPrometheusPort());
        conf.addProperty("prometheusStatsLatencyRolloverSeconds",
            kafkaServiceConfiguration.getKopPrometheusStatsLatencyRolloverSeconds());
        conf.addProperty("prometheusBasicStatsEnable", false);
    }

    public StatsProvider getStatsProvider() {
        return this.statsProvider;
    }

    protected void start() {
        statsProvider.start(conf);
    }

    protected void stop() {
        statsProvider.stop();
    }

}
