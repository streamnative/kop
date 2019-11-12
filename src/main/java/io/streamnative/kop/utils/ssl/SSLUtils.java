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
package io.streamnative.kop.utils.ssl;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.streamnative.kop.KafkaServiceConfiguration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLEngine;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Helper class for setting up SSL for KafkaChannelInitializer.
 */
public class SSLUtils {
    // A map between kafka SslConfigs and KakaServiceConfiguration.
    public static final Map<String, String> CONFIG_NAME_MAP = ImmutableMap.<String, String>builder()
        .put(SslConfigs.SSL_PROTOCOL_CONFIG, "kopSslProtocol")
        .put(SslConfigs.SSL_PROVIDER_CONFIG, "kopSslProvider")
        .put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "kopSslCipherSuites")
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
        .build();

    public static SslContextFactory createSslContextFactory(KafkaServiceConfiguration kafkaServiceConfiguration) {
        Properties properties = kafkaServiceConfiguration.getProperties();
        Builder<String, Object> sslConfigValues = ImmutableMap.builder();

        CONFIG_NAME_MAP.forEach((key, value) -> {
            Object obj = properties.getProperty(value);
            if (obj != null) {
                sslConfigValues.put(key, obj);
            }
        });
        return createSslContextFactory(sslConfigValues.build());
    }

    public static SslContextFactory createSslContextFactory(Map<String, Object> sslConfigValues) {
        SslContextFactory ssl = new SslContextFactory();

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
        List<String> sslEnabledProtocols =
            (List<String>) getOrDefault(
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

        List<String> sslCipherSuites = (List<String>) sslConfigValues.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
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
    protected static void configureSslContextFactoryAuthentication(SslContextFactory ssl,
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
    public static SSLEngine createSslEngine(SslContextFactory sslContextFactory) throws Exception {
        sslContextFactory.start();
        SSLEngine engine  = sslContextFactory.newSSLEngine();
        engine.setUseClientMode(false);

        return engine;
    }

}
