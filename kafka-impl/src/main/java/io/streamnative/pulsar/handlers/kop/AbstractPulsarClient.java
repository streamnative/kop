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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.Closeable;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;


/**
 * The abstraction wrapper of {@link PulsarClientImpl}.
 */
@Slf4j
@Getter
public abstract class AbstractPulsarClient implements Closeable {

    private final PulsarClientImpl pulsarClient;

    public AbstractPulsarClient(@NonNull final PulsarClientImpl pulsarClient) {
        this.pulsarClient = pulsarClient;
    }

    @Override
    public void close() {
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close PulsarClient of {}", getClass().getTypeName(), e);
        }
    }

    protected static PulsarClientImpl createPulsarClient(final PulsarService pulsarService) {
        try {
            return (PulsarClientImpl) pulsarService.getClient();
        } catch (PulsarServerException e) {
            log.error("Failed to create PulsarClient", e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Create a Pulsar client.
     * @param pulsarService the PulsarService for the client
     * @param kafkaConfig the Kafka config object
     * @param customConfig a custom Consumer for setting values in kafkaConfig
     * @return the created Pulsar client
     */
    public static PulsarClientImpl createPulsarClient(final PulsarService pulsarService,
                                                         final KafkaServiceConfiguration kafkaConfig,
                                                         final Consumer<ClientConfigurationData> customConfig) {
        // It's migrated from PulsarService#getClient()
        final ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(kafkaConfig.isTlsEnabled()
                ? pulsarService.getBrokerServiceUrlTls()
                : pulsarService.getBrokerServiceUrl());
        conf.setTlsAllowInsecureConnection(kafkaConfig.isTlsAllowInsecureConnection());
        conf.setTlsTrustCertsFilePath(kafkaConfig.getTlsTrustCertsFilePath());

        if (kafkaConfig.isBrokerClientTlsEnabled()) {
            if (kafkaConfig.isBrokerClientTlsEnabledWithKeyStore()) {
                conf.setUseKeyStoreTls(true);
                conf.setTlsTrustStoreType(kafkaConfig.getBrokerClientTlsTrustStoreType());
                conf.setTlsTrustStorePath(kafkaConfig.getBrokerClientTlsTrustStore());
                conf.setTlsTrustStorePassword(kafkaConfig.getBrokerClientTlsTrustStorePassword());
            } else {
                conf.setTlsTrustCertsFilePath(
                        isNotBlank(kafkaConfig.getBrokerClientTrustCertsFilePath())
                                ? kafkaConfig.getBrokerClientTrustCertsFilePath()
                                : kafkaConfig.getTlsTrustCertsFilePath());
            }
        }

        try {
            if (isNotBlank(kafkaConfig.getBrokerClientAuthenticationPlugin())) {
                conf.setAuthPluginClassName(kafkaConfig.getBrokerClientAuthenticationPlugin());
                conf.setAuthParams(kafkaConfig.getBrokerClientAuthenticationParameters());
                conf.setAuthParamMap(null);
                conf.setAuthentication(AuthenticationFactory.create(
                        kafkaConfig.getBrokerClientAuthenticationPlugin(),
                        kafkaConfig.getBrokerClientAuthenticationParameters()));
            }

            customConfig.accept(conf);
            return new PulsarClientImpl(conf);
        } catch (PulsarClientException e) {
            log.error("Failed to create PulsarClient", e);
            throw new IllegalStateException(e);
        }
    }
}
