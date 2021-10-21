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
package io.streamnative.pulsar.handlers.kop.streams;

import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;

public class MockStoreBuilder extends AbstractStoreBuilder<Integer, byte[], StateStore> {

    private final boolean persistent;

    public MockStoreBuilder(final String storeName, final boolean persistent) {
        super(storeName, Serdes.Integer(), Serdes.ByteArray(), new MockTime());

        this.persistent = persistent;
    }

    @Override
    public StateStore build() {
        return new MockStateStore(name, persistent);
    }
}

