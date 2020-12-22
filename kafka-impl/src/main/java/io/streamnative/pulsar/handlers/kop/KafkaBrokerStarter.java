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


import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.DirectMemoryUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * A starter to start Kafka-on-Pulsar broker.
 */
@Slf4j
public class KafkaBrokerStarter {

    private static class BrokerStarter {
        private final KafkaServiceConfiguration brokerConfig;
        private final KafkaService kafkaService;
        private final BookieServer bookieServer;
        private final AutoRecoveryMain autoRecoveryMain;
        private final StatsProvider bookieStatsProvider;
        private final ServerConfiguration bookieConfig;

        BrokerStarter(String[] args) throws Exception {
            StarterArguments starterArguments = new StarterArguments();
            JCommander jcommander = new JCommander(starterArguments);
            jcommander.setProgramName("KafkaBrokerStarter");

            // parse args by JCommander
            jcommander.parse(args);
            if (starterArguments.help) {
                jcommander.usage();
                Runtime.getRuntime().exit(-1);
            }

            // init broker config
            if (isBlank(starterArguments.brokerConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("Need to specify a configuration file for kafkaBroker");
            } else {
                brokerConfig = loadConfig(starterArguments.brokerConfigFile);
            }

            int maxFrameSize = brokerConfig.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING;
            if (maxFrameSize >= DirectMemoryUtils.maxDirectMemory()) {
                throw new IllegalArgumentException("Max message size need smaller than jvm directMemory");
            }


            if (brokerConfig.getAdvertisedAddress() != null
                && !brokerConfig.getListeners().contains(brokerConfig.getAdvertisedAddress())) {
                String err = "Error config: advertisedAddress - " + brokerConfig.getAdvertisedAddress()
                    + " and listeners - " + brokerConfig.getListeners() + " not match.";
                log.error(err);
                throw new IllegalArgumentException(err);
            }

            // init kafka broker service
            kafkaService = new KafkaService(brokerConfig);

            // if no argument to run bookie in cmd line, read from pulsar config
            if (!argsContains(args, "-rb") && !argsContains(args, "--run-bookie")) {
                checkState(!starterArguments.runBookie,
                    "runBookie should be false if has no argument specified");
                starterArguments.runBookie = brokerConfig.isEnableRunBookieTogether();
            }
            if (!argsContains(args, "-ra") && !argsContains(args, "--run-bookie-autorecovery")) {
                checkState(!starterArguments.runBookieAutoRecovery,
                    "runBookieAutoRecovery should be false if has no argument specified");
                starterArguments.runBookieAutoRecovery = brokerConfig.isEnableRunBookieAutoRecoveryTogether();
            }

            if ((starterArguments.runBookie || starterArguments.runBookieAutoRecovery)
                && isBlank(starterArguments.bookieConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("No configuration file for Bookie");
            }

            // init stats provider
            if (starterArguments.runBookie || starterArguments.runBookieAutoRecovery) {
                checkState(isNotBlank(starterArguments.bookieConfigFile),
                    "No configuration file for Bookie");
                bookieConfig = readBookieConfFile(starterArguments.bookieConfigFile);
                Class<? extends StatsProvider> statsProviderClass = bookieConfig.getStatsProviderClass();
                bookieStatsProvider = ReflectionUtils.newInstance(statsProviderClass);
            } else {
                bookieConfig = null;
                bookieStatsProvider = null;
            }

            // init bookie server
            if (starterArguments.runBookie) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie");
                checkNotNull(bookieStatsProvider, "No Stats Provider for Bookie");
                bookieServer = new BookieServer(
                        bookieConfig, bookieStatsProvider.getStatsLogger(""), BookieServiceInfo.NO_INFO);
            } else {
                bookieServer = null;
            }

            // init bookie AutorecoveryMain
            if (starterArguments.runBookieAutoRecovery) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie Autorecovery");
                autoRecoveryMain = new AutoRecoveryMain(bookieConfig);
            } else {
                autoRecoveryMain = null;
            }
        }

        public void start() throws Exception {
            if (bookieStatsProvider != null) {
                bookieStatsProvider.start(bookieConfig);
                log.info("started bookieStatsProvider.");
            }
            if (bookieServer != null) {
                bookieServer.start();
                log.info("started bookieServer.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.start();
                log.info("started bookie autoRecoveryMain.");
            }

            kafkaService.start();
            log.info("KafkaService started.");
        }

        public void join() throws InterruptedException {
            kafkaService.waitUntilClosed();

            if (bookieServer != null) {
                bookieServer.join();
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.join();
            }
        }

        @SuppressFBWarnings("RU_INVOKE_RUN")
        public void shutdown() {
            kafkaService.getShutdownService().run();
            log.info("Shut down kafkaBroker service successfully.");

            if (bookieStatsProvider != null) {
                bookieStatsProvider.stop();
                log.info("Shut down bookieStatsProvider successfully.");
            }
            if (bookieServer != null) {
                bookieServer.shutdown();
                log.info("Shut down bookieServer successfully.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.shutdown();
                log.info("Shut down autoRecoveryMain successfully.");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s",
                dateFormat.format(new Date()),
                thread.getContextClassLoader(),
                thread.getName(),
                exception.getMessage()));
        });

        BrokerStarter starter = new BrokerStarter(args);
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                starter.shutdown();
            })
        );

        PulsarByteBufAllocator.registerOOMListener(oomException -> {
            log.error("-- Shutting down - Received OOM exception: {}", oomException.getMessage(), oomException);
            starter.shutdown();
        });

        try {
            starter.start();
        } catch (Exception e) {
            log.error("Failed to start pulsar service.", e);
            Runtime.getRuntime().halt(1);
        }

        starter.join();
    }

    @VisibleForTesting
    private static class StarterArguments {
        @Parameter(names = {
            "-c", "--kop-conf"
        }, description = "Configuration file for Kafka on Pulsar Broker")
        private String brokerConfigFile =
            Paths.get("").toAbsolutePath().normalize().toString() + "/conf/kop.conf";

        @Parameter(names = {
            "-rb", "--run-bookie"
        }, description = "Run Bookie together with Broker")
        private boolean runBookie = false;

        @Parameter(names = {
            "-ra", "--run-bookie-autorecovery"
        }, description = "Run Bookie Autorecovery together with kafkaBroker")
        private boolean runBookieAutoRecovery = false;

        @Parameter(names = {
            "-bc", "--bookie-conf"
        }, description = "Configuration file for Bookie")
        private String bookieConfigFile =
            Paths.get("").toAbsolutePath().normalize().toString() + "/conf/bookkeeper.conf";

        @Parameter(names = {
            "-h", "--help"
        }, description = "Show this help message")
        private boolean help = false;
    }

    private static boolean argsContains(String[] args, String arg) {
        return Arrays.asList(args).contains(arg);
    }

    private static KafkaServiceConfiguration loadConfig(String configFile) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        KafkaServiceConfiguration config = ConfigurationUtils.create(
            new FileInputStream(configFile),
            KafkaServiceConfiguration.class);
        // it validates provided configuration is completed
        isComplete(config);
        return config;
    }

    private static ServerConfiguration readBookieConfFile(String bookieConfigFile) throws IllegalArgumentException {
        ServerConfiguration bookieConf = new ServerConfiguration();
        try {
            bookieConf.loadConf(new File(bookieConfigFile).toURI().toURL());
            bookieConf.validate();
            log.info("Using bookie configuration file {}", bookieConfigFile);
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Could not open configuration file");
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Malformed configuration file");
        }

        if (bookieConf.getMaxPendingReadRequestPerThread() < bookieConf.getRereplicationEntryBatchSize()) {
            throw new IllegalArgumentException(
                "rereplicationEntryBatchSize should be smaller than " + "maxPendingReadRequestPerThread");
        }
        return bookieConf;
    }
}
