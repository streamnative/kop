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
