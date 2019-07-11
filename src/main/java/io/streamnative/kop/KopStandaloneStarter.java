package io.streamnative.kop;


import static org.apache.commons.lang3.StringUtils.isBlank;

import com.beust.jcommander.JCommander;
import java.io.FileInputStream;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;

@Slf4j
public class KopStandaloneStarter extends KopStandalone {

    public KopStandaloneStarter(String[] args) throws Exception {

        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(this);
            jcommander.parse(args);
            if (this.isHelp() || isBlank(this.getConfigFile())) {
                jcommander.usage();
                return;
            }

            if (this.isNoBroker() && this.isOnlyBroker()) {
                log.error("Only one option is allowed between '--no-broker' and '--only-broker'");
                jcommander.usage();
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            log.error(e.getMessage());
            return;
        }

        this.config = PulsarConfigurationLoader.create((new FileInputStream(this.getConfigFile())), ServiceConfiguration.class);

        String zkServers = "127.0.0.1";

        if (this.getAdvertisedAddress() != null) {
            // Use advertised address from command line
            config.setAdvertisedAddress(this.getAdvertisedAddress());
            zkServers = this.getAdvertisedAddress();
        } else if (isBlank(config.getAdvertisedAddress())) {
            // Use advertised address as local hostname
            config.setAdvertisedAddress(ServiceConfigurationUtils.unsafeLocalhostResolve());
        } else {
            // Use advertised address from config file
        }

        // Set ZK server's host to localhost
        // Priority: args > conf > default
        if (argsContains(args,"--zookeeper-port")) {
            config.setZookeeperServers(zkServers + ":" + this.getZkPort());
        } else {
            if (config.getZookeeperServers() != null) {
                this.setZkPort(Integer.parseInt(config.getZookeeperServers().split(":")[1]));
            }
            config.setZookeeperServers(zkServers + ":" + this.getZkPort());
        }

        if (config.getConfigurationStoreServers() == null) {
            config.setConfigurationStoreServers(zkServers + ":" + this.getZkPort());
        }

        config.setRunningStandalone(true);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    if (kopBroker != null) {
                        kopBroker.close();
                    }

                    if (bkEnsemble != null) {
                        bkEnsemble.stop();
                    }
                } catch (Exception e) {
                    log.error("Shutdown failed: {}", e.getMessage());
                }
            }
        });
    }

    private static boolean argsContains(String[] args, String arg) {
        return Arrays.asList(args).contains(arg);
    }

    public static void main(String args[]) throws Exception {
        // Start standalone
        KopStandaloneStarter standalone = new KopStandaloneStarter(args);
        try {
            standalone.start();
        } catch (Throwable th) {
            log.error("Failed to start pulsar service.", th);
            Runtime.getRuntime().exit(1);
        }

    }
}
