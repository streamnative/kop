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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.common.naming.TopicName;

/**
 * This is a listener to receive notifications about UNLOAD and DELETE events.
 * The TopicEventsListener API is available only since Pulsar 3.0.0
 * and Luna Streaming 2.10.3.3. This is the reason why this class is not an innerclass
 * of {@link NamespaceBundleOwnershipListenerImpl}, because we don't want to load it and
 * cause errors on older versions of Pulsar.
 *
 * Please note that we are not interested in LOAD events because they are handled
 * in {@link NamespaceBundleOwnershipListenerImpl} in a different way.
 */
@Slf4j
class TopicEventListenerImpl implements TopicEventsListener {

    final NamespaceBundleOwnershipListenerImpl parent;

    public TopicEventListenerImpl(NamespaceBundleOwnershipListenerImpl parent) {
        this.parent = parent;
    }

    @Override
    public void handleEvent(String topicName, TopicEvent event, EventStage stage, Throwable t) {
        if (stage == EventStage.SUCCESS || stage == EventStage.FAILURE) {
            if (log.isDebugEnabled()) {
                log.debug("handleEvent {} {} on {}", event, stage, topicName);
            }
            TopicName topicName1 = TopicName.get(topicName);
            switch (event) {
                case UNLOAD:
                    parent.notifyUnloadTopic(topicName1.getNamespaceObject(), topicName1);
                    break;
                case DELETE:
                    parent.notifyDeleteTopic(topicName1.getNamespaceObject(), topicName1);
                    break;
                default:
                    if (log.isDebugEnabled()) {
                        log.debug("Ignore event {} {} on {}", event, stage, topicName);
                    }
                    break;
            }
        }
    }
}
