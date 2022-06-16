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
package io.streamnative.pulsar.handlers.kop.utils.ssl;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Helper class for setting up SSL for KafkaChannelInitializer.
 * Kafka and Pulsar use different way to store SSL keys, this utils only work for Kafka.
 */
@Slf4j
public class SSLUtils {
    // A map between kafka SslConfigs and KafkaServiceConfiguration.
    public static final Map<String, String> CONFIG_NAME_MAP = ImmutableMap.<String, String>builder()
            .put(SslConfigs.SSL_PROTOCOL_CONFIG, "kopSslProtocol")
            .put(SslConfigs.SSL_PROVIDER_CONFIG, "kopSslProvider")
            .put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "kopSslCipherSuites")
            .put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "kopSslEnabledProtocols")
            .put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "kopSslKeystoreType")
            .put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "kopSslKeystoreLocation")
            .put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "kopSslKeystorePassword")
            .put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "kopSslKeyPassword")
            .put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "kopSslTruststoreType")
            .put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "kopSslTruststoreLocation")
            .put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kopSslTruststorePassword")
            .put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, "kopSslKeymanagerAlgorithm")
            .put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "kopSslTrustmanagerAlgorithm")
            .put(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, "kopSslSecureRandomImplementation")
            .put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "kopSslClientAuth")
            .build();

    public static SslContextFactory.Server createSslContextFactory(
            KafkaServiceConfiguration kafkaServiceConfiguration) {
        Builder<String, Object> sslConfigValues = ImmutableMap.builder();

        CONFIG_NAME_MAP.forEach((key, value) -> {
            Object obj = null;
            switch(key) {
                case SslConfigs.SSL_PROTOCOL_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslProtocol();
                    break;
                case SslConfigs.SSL_PROVIDER_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslProvider();
                    break;
                case SslConfigs.SSL_CIPHER_SUITES_CONFIG:
                    // this obj is Set<String>
                    obj = kafkaServiceConfiguration.getKopSslCipherSuites();
                    break;
                case SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslEnabledProtocols();
                    break;
                case SslConfigs.SSL_KEYSTORE_TYPE_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeystoreType();
                    break;
                case SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeystoreLocation();
                    break;
                case SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeystorePassword();
                    break;
                case SslConfigs.SSL_KEY_PASSWORD_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeyPassword();
                    break;
                case SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTruststoreType();
                    break;
                case SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTruststoreLocation();
                    break;
                case SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTruststorePassword();
                    break;
                case SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeymanagerAlgorithm();
                    break;
                case SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTrustmanagerAlgorithm();
                    break;
                case SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslSecureRandomImplementation();
                    break;
                case BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslClientAuth();
                    break;
                default:
                    log.error("key {} not contained in KafkaServiceConfiguration", key);
            }
            if (obj != null) {
                sslConfigValues.put(key, obj);
            }
        });
        return createSslContextFactory(sslConfigValues.build());
    }

    public static SslContextFactory.Server createSslContextFactory(Map<String, Object> sslConfigValues) {
        SslContextFactory.Server ssl = new SslContextFactory.Server();

        configureSslContextFactoryKeyStore(ssl, sslConfigValues);
        configureSslContextFactoryTrustStore(ssl, sslConfigValues);
        configureSslContextFactoryAlgorithms(ssl, sslConfigValues);
        configureSslContextFactoryAuthentication(ssl, sslConfigValues);
        ssl.setEndpointIdentificationAlgorithm(null);
        return ssl;
    }

    /**
     * Configures KeyStore related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryKeyStore(SslContextFactory ssl,
                                                             Map<String, Object> sslConfigValues) {
        ssl.setKeyStoreType((String)
            getOrDefault(sslConfigValues, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE));

        String sslKeystoreLocation = (String) sslConfigValues.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (sslKeystoreLocation != null) {
            ssl.setKeyStorePath(sslKeystoreLocation);
        }

        Password sslKeystorePassword =
            new Password((String) sslConfigValues.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        if (sslKeystorePassword != null) {
            ssl.setKeyStorePassword(sslKeystorePassword.value());
        }

        Password sslKeyPassword =
            new Password((String) sslConfigValues.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        if (sslKeyPassword != null) {
            ssl.setKeyManagerPassword(sslKeyPassword.value());
        }
    }

    protected static Object getOrDefault(Map<String, Object> configMap, String key, Object defaultValue) {
        if (configMap.containsKey(key)) {
            return configMap.get(key);
        }

        return defaultValue;
    }

    /**
     * Configures TrustStore related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryTrustStore(SslContextFactory ssl,
                                                               Map<String, Object> sslConfigValues) {
        ssl.setTrustStoreType(
            (String) getOrDefault(
                sslConfigValues,
                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE));

        String sslTruststoreLocation = (String) sslConfigValues.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        if (sslTruststoreLocation != null) {
            ssl.setTrustStorePath(sslTruststoreLocation);
        }

        Password sslTruststorePassword =
            new Password((String) sslConfigValues.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        if (sslTruststorePassword != null) {
            ssl.setTrustStorePassword(sslTruststorePassword.value());
        }
    }

    /**
     * Configures Protocol, Algorithm and Provider related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryAlgorithms(SslContextFactory ssl,
                                                               Map<String, Object> sslConfigValues) {
        Set<String> sslEnabledProtocols =
            (Set<String>) getOrDefault(
                sslConfigValues,
                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")));
        ssl.setIncludeProtocols(sslEnabledProtocols.toArray(new String[sslEnabledProtocols.size()]));

        String sslProvider = (String) sslConfigValues.get(SslConfigs.SSL_PROVIDER_CONFIG);
        if (sslProvider != null) {
            ssl.setProvider(sslProvider);
        }

        ssl.setProtocol(
            (String) getOrDefault(sslConfigValues, SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL));

        Set<String> sslCipherSuites = (Set<String>) sslConfigValues.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (sslCipherSuites != null) {
            ssl.setIncludeCipherSuites(sslCipherSuites.toArray(new String[sslCipherSuites.size()]));
        }

        ssl.setKeyManagerFactoryAlgorithm((String) getOrDefault(
            sslConfigValues,
            SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
            SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM));

        String sslSecureRandomImpl = (String) sslConfigValues.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        if (sslSecureRandomImpl != null) {
            ssl.setSecureRandomAlgorithm(sslSecureRandomImpl);
        }

        ssl.setTrustManagerFactoryAlgorithm((String) getOrDefault(
            sslConfigValues,
            SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
            SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM));
    }

    /**
     * Configures Authentication related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryAuthentication(SslContextFactory.Server ssl,
                                                                   Map<String, Object> sslConfigValues) {
        String sslClientAuth = (String) getOrDefault(
            sslConfigValues,
            BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
            "none");
        switch (sslClientAuth) {
            case "requested":
                ssl.setWantClientAuth(true);
                break;
            case "required":
                ssl.setNeedClientAuth(true);
                break;
            default:
                ssl.setNeedClientAuth(false);
                ssl.setWantClientAuth(false);
        }
    }

    /**
     * Create SSL engine used in KafkaChannelInitializer.
     */
    public static SSLEngine createSslEngine(SslContextFactory.Server sslContextFactory) throws Exception {
        sslContextFactory.start();
        SSLEngine engine  = sslContextFactory.newSSLEngine();
        engine.setUseClientMode(false);

        return engine;
    }

    public static SSLEngine createClientSslEngine(SslContextFactory.Client sslContextFactory) throws Exception {
        sslContextFactory.start();
        SSLEngine engine  = sslContextFactory.newSSLEngine();
        engine.setUseClientMode(true);

        return engine;
    }

    public static SslContextFactory.Client createClientSslContextFactory(
            KafkaServiceConfiguration kafkaServiceConfiguration) {
        Builder<String, Object> sslConfigValues = ImmutableMap.builder();

        CONFIG_NAME_MAP.forEach((key, value) -> {
            Object obj = null;
            switch(key) {
                case SslConfigs.SSL_PROTOCOL_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslProtocol();
                    break;
                case SslConfigs.SSL_PROVIDER_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslProvider();
                    break;
                case SslConfigs.SSL_CIPHER_SUITES_CONFIG:
                    // this obj is Set<String>
                    obj = kafkaServiceConfiguration.getKopSslCipherSuites();
                    break;
                case SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslEnabledProtocols();
                    break;
                case SslConfigs.SSL_KEYSTORE_TYPE_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeystoreType();
                    break;
                case SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeystoreLocation();
                    break;
                case SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeystorePassword();
                    break;
                case SslConfigs.SSL_KEY_PASSWORD_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeyPassword();
                    break;
                case SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTruststoreType();
                    break;
                case SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTruststoreLocation();
                    break;
                case SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTruststorePassword();
                    break;
                case SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslKeymanagerAlgorithm();
                    break;
                case SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslTrustmanagerAlgorithm();
                    break;
                case SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslSecureRandomImplementation();
                    break;
                case BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG:
                    obj = kafkaServiceConfiguration.getKopSslClientAuth();
                    break;
                default:
                    log.error("key {} not contained in KafkaServiceConfiguration", key);
            }
            if (obj != null) {
                sslConfigValues.put(key, obj);
            }
        });
        return createClientSslContextFactory(sslConfigValues.build());
    }

    public static SslContextFactory.Client createClientSslContextFactory(Map<String, Object> sslConfigValues) {
        SslContextFactory.Client ssl = new SslContextFactory.Client();
        configureSslContextFactoryTrustStore(ssl, sslConfigValues);
        configureSslContextFactoryAlgorithms(ssl, sslConfigValues);
        ssl.setEndpointIdentificationAlgorithm(null);
        return ssl;
    }

}
