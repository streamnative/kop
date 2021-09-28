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

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;

/**
 * The client that is responsible for topic lookup.
 */
@Slf4j
public class LookupClient extends AbstractPulsarClient {

    private final NamespaceService namespaceService;

    private ConcurrentHashMap<String, PulsarClientImpl> pulsarClientMap;

    public LookupClient(final PulsarService pulsarService, final KafkaServiceConfiguration kafkaConfig) {
        super(createPulsarClient(pulsarService, kafkaConfig, conf -> {}));
        namespaceService = pulsarService.getNamespaceService();
        try {
            pulsarClientMap = createPulsarClientMap(pulsarService, kafkaConfig);
        } catch (PulsarClientException e) {
            log.error("Failed to create PulsarClient", e);
            throw new IllegalStateException(e);
        }
    }

    public LookupClient(final PulsarService pulsarService) {
        super(createPulsarClient(pulsarService));
        log.warn("This constructor should not be called, it's only called "
                + "when the PulsarService doesn't exist in KafkaProtocolHandlers.LOOKUP_CLIENT_UP");
        namespaceService = pulsarService.getNamespaceService();
    }

    public CompletableFuture<InetSocketAddress> getBrokerAddress(final TopicName topicName) {
        return getBrokerAddress(topicName, null);
    }

    public CompletableFuture<InetSocketAddress> getBrokerAddress(final TopicName topicName, String listenerName) {
        // First try to use NamespaceService to find the broker directly.
        final LookupOptions options = LookupOptions.builder()
                .authoritative(false)
                .advertisedListenerName(listenerName)
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
                return pulsarClientMap.getOrDefault(listenerName == null ? "" : listenerName, getPulsarClient()).
                        getLookup().getBroker(topicName).thenApply(Pair::getLeft);
            } else {
                return getAddressFutureFromBrokerUrl(lookupResult.getLookupData().getBrokerUrl());
            }
        });
    }

    private ConcurrentHashMap<String, PulsarClientImpl> createPulsarClientMap(
            PulsarService pulsarService, KafkaServiceConfiguration kafkaConfig) throws PulsarClientException {
        ConcurrentHashMap<String, PulsarClientImpl> pulsarClientMap = new ConcurrentHashMap<>();
        final Map<String, SecurityProtocol> protocolMap = EndPoint.parseProtocolMap(kafkaConfig.getKafkaProtocolMap());
        if (protocolMap.isEmpty()) {
            pulsarClientMap.put("", getPulsarClient());
        } else {
            for (Map.Entry<String, SecurityProtocol> entry : protocolMap.entrySet()) {
                pulsarClientMap.put(entry.getKey(), createPulsarClient(
                        pulsarService, kafkaConfig, conf -> conf.setListenerName(entry.getKey())));
            }
        }
        return pulsarClientMap;
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
