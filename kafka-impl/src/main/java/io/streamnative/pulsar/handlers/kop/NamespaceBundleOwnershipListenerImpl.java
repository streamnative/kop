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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@AllArgsConstructor
@Slf4j
public class NamespaceBundleOwnershipListenerImpl {

    private static final boolean USE_TOPIC_EVENT_LISTENER;
    static {
        boolean isTopicEventsListenerAvailable = false;
        try {
            Class.forName("org.apache.pulsar.broker.service.TopicEventsListener",
                    true, BrokerService.class.getClassLoader());
            log.info("Detected TopicEventsListener API");
            isTopicEventsListenerAvailable = true;
        } catch (ClassNotFoundException legacyPulsarVersion) {
            log.info("TopicEventsListener API is not available on this version of Pulsar");
        }
        USE_TOPIC_EVENT_LISTENER = isTopicEventsListenerAvailable;
    }

    private final List<TopicOwnershipListener> topicOwnershipListeners = new CopyOnWriteArrayList<>();
    private final NamespaceService namespaceService;
    private final BrokerService brokerService;
    private final String brokerUrl;

    private final InnerNamespaceBundleOwnershipListener bundleBasedImpl = new InnerNamespaceBundleOwnershipListener();

    public NamespaceBundleOwnershipListenerImpl(BrokerService brokerService) {
        this.brokerService = brokerService;
        this.brokerUrl =
                brokerService.pulsar().getBrokerServiceUrl();
        this.namespaceService = brokerService.pulsar().getNamespaceService();
    }

    /**
     * @implNote Like {@link NamespaceService#addNamespaceBundleOwnershipListener}, when a new listener is added, the
     * `onLoad` method should be called on each owned bundle if `test(bundle)` returns true.
     */
    public void addTopicOwnershipListener(final TopicOwnershipListener listener) {
        topicOwnershipListeners.add(listener);
        namespaceService.getOwnedServiceUnits()
                .stream()
                .filter(bundleBasedImpl).forEach(bundleBasedImpl::onLoad);
    }

    private boolean anyListenerInterestedInEvent(NamespaceName namespaceName, TopicOwnershipListener.EventType event) {
        return topicOwnershipListeners
                .stream()
                .anyMatch(l -> l.interestedInEvent(namespaceName, event));
    }

    private class InnerNamespaceBundleOwnershipListener implements NamespaceBundleOwnershipListener {

        @Override
        public void onLoad(NamespaceBundle bundle) {

            NamespaceName namespaceObject = bundle.getNamespaceObject();
            if (!anyListenerInterestedInEvent(namespaceObject, TopicOwnershipListener.EventType.LOAD)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Load bundle: {} - NO LISTENER INTERESTED", brokerUrl, bundle);
                }
                return;
            }
            log.info("[{}] Load bundle: {}", brokerUrl, bundle);

            // We have to eagerly list all the topics in the bundle even if they are not LOADED yet,
            // this is necessary in order to eagerly bootstrap the GroupCoordinator and the TransactionCoordinator
            // services.
            getOwnedPersistentTopicList(bundle).thenAccept(topics -> {
                notifyLoadTopics(namespaceObject, topics);
            }).exceptionally(ex -> {
                log.error("[{}] Failed to get owned topic list of {}", brokerUrl, bundle, ex);
                return null;
            });
        }

        @Override
        public void unLoad(NamespaceBundle bundle) {
            if (USE_TOPIC_EVENT_LISTENER) {
                // Unload events hard dispatched in a better way using the TopicEventListener API.
                return;
            }
            // We have to eagerly list all the topics in the bundle, even if they are not LOADED yet
            NamespaceName namespaceObject = bundle.getNamespaceObject();
            if (!anyListenerInterestedInEvent(namespaceObject, TopicOwnershipListener.EventType.UNLOAD)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Unload bundle: {} - NO LISTENER INTERESTED", brokerUrl, bundle);
                }
                return;
            }
            log.info("[{}] Unload bundle: {}", brokerUrl, bundle);
            getOwnedPersistentTopicList(bundle).thenAccept(topics -> {
                notifyUnloadTopics(namespaceObject, topics);
            }).exceptionally(ex -> {
                log.error("[{}] Failed to get owned topic list of {}", brokerUrl, bundle, ex);
                return null;
            });
        }

        @Override
        public boolean test(NamespaceBundle bundle) {
            return true;
        }

        // Kafka topics are always persistent so there is no need to get owned non-persistent topics.
        // However, `NamespaceService#getOwnedTopicListForNamespaceBundle` calls `getFullListTopics`, which always calls
        // `getListOfNonPersistentTopics`. So this method is a supplement to the existing NamespaceService API.
        private CompletableFuture<List<TopicName>> getOwnedPersistentTopicList(final NamespaceBundle bundle) {
            final NamespaceName namespaceName = bundle.getNamespaceObject();
            final CompletableFuture<List<TopicName>> topicsFuture =
                    namespaceService.getListOfPersistentTopics(namespaceName)
                            .thenApply(topics -> topics.stream()
                                    .map(TopicName::get)
                                    .filter(topic -> bundle.includes(topic))
                                    .collect(Collectors.toList()));
            final CompletableFuture<List<TopicName>> partitionsFuture =
                    namespaceService.getPartitions(namespaceName, TopicDomain.persistent)
                            .thenApply(topics -> topics.stream()
                                    .map(TopicName::get)
                                    .filter(topic -> bundle.includes(topic))
                                    .collect(Collectors.toList()));
            return topicsFuture.thenCombine(partitionsFuture, (topics, partitions) -> {
                for (TopicName partition : partitions) {
                    if (!topics.contains(partition)) {
                        topics.add(partition);
                    }
                }
                return topics;
            });
        }
    }

    void notifyUnloadTopic(NamespaceName namespaceObject, TopicName topic) {
        topicOwnershipListeners.forEach(listener -> {
            if (!listener.interestedInEvent(namespaceObject, TopicOwnershipListener.EventType.UNLOAD)) {
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Trigger unload callback for {}", brokerUrl, listener.name(), topic);
            }
            listener.whenUnload(topic);
        });
    }

    void notifyDeleteTopic(NamespaceName namespaceObject, TopicName topic) {
        topicOwnershipListeners.forEach(listener -> {
            if (!listener.interestedInEvent(namespaceObject, TopicOwnershipListener.EventType.DELETE)) {
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Trigger delete callback for {}", brokerUrl, listener.name(), topic);
            }
            listener.whenDelete(topic);
        });
    }

    void notifyUnloadTopics(NamespaceName namespaceObject, List<TopicName> topics) {
        topicOwnershipListeners.forEach(listener -> {
            if (!listener.interestedInEvent(namespaceObject, TopicOwnershipListener.EventType.UNLOAD)) {
                return;
            }
            topics.forEach(topic -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Trigger unload callback for {}", brokerUrl, listener.name(), topic);
                }
                listener.whenUnload(topic);
            });
        });
    }

    private void notifyLoadTopics(NamespaceName namespaceObject, List<TopicName> topics) {
        topicOwnershipListeners.forEach(listener -> {
            if (!listener.interestedInEvent(namespaceObject, TopicOwnershipListener.EventType.LOAD)) {
                return;
            }
            topics.forEach(topic -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Trigger load callback for {}", brokerUrl, listener.name(), topic);
                }
                listener.whenLoad(topic);
            });
        });
    }

    public void register() {
        namespaceService.addNamespaceBundleOwnershipListener(bundleBasedImpl);
        if (USE_TOPIC_EVENT_LISTENER) {
            brokerService.addTopicEventListener(new TopicEventListenerImpl(this));
        }
    }

}
