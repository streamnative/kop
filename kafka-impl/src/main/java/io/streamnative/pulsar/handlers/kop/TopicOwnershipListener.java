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

import java.util.function.Predicate;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Listener that is triggered when a topic's ownership changed via load or unload.
 */
public interface TopicOwnershipListener extends Predicate<NamespaceName> {

    /**
     * It's triggered when the topic is loaded by a broker.
     *
     * @param topicName
     */
    void whenLoad(TopicName topicName);

    /**
     * It's triggered when the topic is unloaded by a broker.
     *
     * @param topicName
     */
    void whenUnload(TopicName topicName);

    /** Returns the name of the listener. */
    String name();

    default boolean test(NamespaceName namespaceName) {
        return true;
    }
}
