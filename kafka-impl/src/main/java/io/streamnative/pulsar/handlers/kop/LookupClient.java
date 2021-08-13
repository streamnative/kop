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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;

/**
 * The client that is responsible for topic lookup.
 */
@Slf4j
public class LookupClient implements Closeable {

    private final NamespaceService namespaceService;
    @Getter
    private final PulsarClientImpl pulsarClient;

    public LookupClient(final PulsarService pulsarService, final KafkaServiceConfiguration kafkaConfig) {
        namespaceService = pulsarService.getNamespaceService();
        try {
            pulsarClient = createPulsarClient(pulsarService, kafkaConfig);
        } catch (PulsarClientException e) {
            log.error("Failed to create PulsarClient", e);
            throw new IllegalStateException(e);
        }
    }

    public LookupClient(final PulsarService pulsarService) {
        log.warn("This constructor should not be called, it's only called "
                + "when the PulsarService doesn't exist in KafkaProtocolHandlers.LOOKUP_CLIENT_UP");
        namespaceService = pulsarService.getNamespaceService();
        try {
            pulsarClient = (PulsarClientImpl) pulsarService.getClient();
        } catch (PulsarServerException e) {
            log.error("Failed to create PulsarClient", e);
            throw new IllegalStateException(e);
        }
    }

    public CompletableFuture<InetSocketAddress> getBrokerAddress(final TopicName topicName) {
        // First try to use NamespaceService to find the broker directly.
        final LookupOptions options = LookupOptions.builder()
                .authoritative(false)
                .advertisedListenerName(pulsarClient.getConfiguration().getListenerName())
                .loadTopicsInBundle(true)
                .build();
        return namespaceService.getBrokerServiceUrlAsync(topicName, options).thenCompose(optLookupResult -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Lookup result {}", topicName.toString(), optLookupResult);
            }
            if (!optLookupResult.isPresent()) {
                return getFailedAddressFuture(ClientCnx.getPulsarClientException(
                        ServerError.ServiceNotReady,
                        "No broker was available to own " + topicName));
            }
            final LookupResult lookupResult = optLookupResult.get();
            if (lookupResult.isRedirect()) {
                // Kafka client can't process redirect field, so here we fallback to PulsarClient
                return pulsarClient.getLookup().getBroker(topicName).thenApply(Pair::getLeft);
            } else {
                return getAddressFutureFromBrokerUrl(lookupResult.getLookupData().getBrokerUrl());
            }
        });
    }

    @Override
    public void close() {
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close PulsarClient of LookupClient", e);
        }
    }

    private static PulsarClientImpl createPulsarClient(final PulsarService pulsarService,
                                                       final KafkaServiceConfiguration kafkaConfig)
            throws PulsarClientException {
        // It's migrated from PulsarService#getClient() but it can configure listener name
        final ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(kafkaConfig.isTlsEnabled()
                ? pulsarService.getBrokerServiceUrlTls()
                : pulsarService.getBrokerServiceUrl());
        conf.setTlsAllowInsecureConnection(kafkaConfig.isTlsAllowInsecureConnection());
        conf.setTlsTrustCertsFilePath(kafkaConfig.getTlsCertificateFilePath());

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
                                : kafkaConfig.getTlsCertificateFilePath());
            }
        }

        if (isNotBlank(kafkaConfig.getBrokerClientAuthenticationPlugin())) {
            conf.setAuthPluginClassName(kafkaConfig.getBrokerClientAuthenticationPlugin());
            conf.setAuthParams(kafkaConfig.getBrokerClientAuthenticationParameters());
            conf.setAuthParamMap(null);
            conf.setAuthentication(AuthenticationFactory.create(
                    kafkaConfig.getBrokerClientAuthenticationPlugin(),
                    kafkaConfig.getBrokerClientAuthenticationParameters()));
        }

        conf.setListenerName(kafkaConfig.getKafkaListenerName());
        return new PulsarClientImpl(conf, pulsarService.getIoEventLoopGroup());
    }

    private static CompletableFuture<InetSocketAddress> getFailedAddressFuture(final Throwable throwable) {
        final CompletableFuture<InetSocketAddress> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    private static CompletableFuture<InetSocketAddress> getAddressFutureFromBrokerUrl(final String brokerUrl) {
        final CompletableFuture<InetSocketAddress> future = new CompletableFuture<>();
        try {
            final URI uri = new URI(brokerUrl);
            future.complete(InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort()));
        } catch (URISyntaxException e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
