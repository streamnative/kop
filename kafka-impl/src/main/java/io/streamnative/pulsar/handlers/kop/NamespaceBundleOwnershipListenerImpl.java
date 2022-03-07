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
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@AllArgsConstructor
@Slf4j
public class NamespaceBundleOwnershipListenerImpl implements NamespaceBundleOwnershipListener {

    private final List<TopicOwnershipListener> topicOwnershipListeners = new CopyOnWriteArrayList<>();
    private final NamespaceService namespaceService;
    private final String brokerUrl;

    /**
     * @implNote Like {@link NamespaceService#addNamespaceBundleOwnershipListener}, when a new listener is added, the
     * `onLoad` method should be called on each owned bundle if `test(bundle)` returns true.
     */
    public void addTopicOwnershipListener(final TopicOwnershipListener listener) {
        topicOwnershipListeners.add(listener);
        namespaceService.getOwnedServiceUnits().stream().filter(this).forEach(this::onLoad);
    }

    @Override
    public void onLoad(NamespaceBundle bundle) {
        log.info("[{}] Load bundle: {}", brokerUrl, bundle);
        getOwnedPersistentTopicList(bundle).whenComplete((topics, e) -> {
            if (e != null) {
                log.error("[{}] Failed to get owned topic list of {}", brokerUrl, bundle, e);
                return;
            }
            topicOwnershipListeners.forEach(listener -> {
                if (!listener.test(bundle.getNamespaceObject())) {
                    return;
                }
                topics.forEach(topic -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Trigger load callback for {}", brokerUrl, listener.name(), topic);
                    }
                    listener.whenLoad(TopicName.get(topic));
                });
            });
        });
    }

    @Override
    public void unLoad(NamespaceBundle bundle) {
        log.info("[{}] Unload bundle: {}", brokerUrl, bundle);
        getOwnedPersistentTopicList(bundle).whenComplete((topics, e) -> {
            if (e != null) {
                log.error("[{}] Failed to get owned topic list of {}", brokerUrl, bundle, e);
                return;
            }
            topicOwnershipListeners.forEach(listener -> {
                if (!listener.test(bundle.getNamespaceObject())) {
                    return;
                }
                topics.forEach(topic -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Trigger unload callback for {}", brokerUrl, listener.name(), topic);
                    }
                    listener.whenUnload(TopicName.get(topic));
                });
            });
        });
    }

    @Override
    public boolean test(NamespaceBundle bundle) {
        return true;
    }

    // Kafka topics are always persistent so there is no need to get owned non-persistent topics.
    // However, `NamespaceService#getOwnedTopicListForNamespaceBundle` calls `getFullListTopics`, which always calls
    // `getListOfNonPersistentTopics`. So this method is a supplement to the existing NamespaceService API.
    private CompletableFuture<List<String>> getOwnedPersistentTopicList(final NamespaceBundle bundle) {
        final NamespaceName namespaceName = bundle.getNamespaceObject();
        final CompletableFuture<List<String>> topicsFuture = namespaceService.getListOfPersistentTopics(namespaceName)
                .thenApply(topics -> topics.stream()
                        .filter(topic -> bundle.includes(TopicName.get(topic)))
                        .collect(Collectors.toList()));
        final CompletableFuture<List<String>> partitionsFuture =
                namespaceService.getPartitions(namespaceName, TopicDomain.persistent)
                        .thenApply(topics -> topics.stream()
                                .filter(topic -> bundle.includes(TopicName.get(topic)))
                                .collect(Collectors.toList()));
        return topicsFuture.thenCombine(partitionsFuture, (topics, partitions) -> {
            for (String partition : partitions) {
                if (!topics.contains(partition)) {
                    topics.add(partition);
                }
            }
            return topics;
        });
    }
}
