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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

public class MockProcessorNode<K, V> extends ProcessorNode<K, V> {

    private static final String NAME = "MOCK-PROCESS-";
    private static final AtomicInteger INDEX = new AtomicInteger(1);

    public final MockProcessor<K, V> mockProcessor;

    public boolean closed;
    public boolean initialized;

    public MockProcessorNode(long scheduleInterval) {
        this(scheduleInterval, PunctuationType.STREAM_TIME);
    }

    public MockProcessorNode(long scheduleInterval, PunctuationType punctuationType) {
        this(new MockProcessor<K, V>(punctuationType, scheduleInterval));
    }

    public MockProcessorNode() {
        this(new MockProcessor<K, V>());
    }

    private MockProcessorNode(final MockProcessor<K, V> mockProcessor) {
        super(NAME + INDEX.getAndIncrement(), mockProcessor, Collections.<String>emptySet());

        this.mockProcessor = mockProcessor;
    }

    @Override
    public void init(final InternalProcessorContext context) {
        super.init(context);
        initialized = true;
    }

    @Override
    public void process(K key, V value) {
        processor().process(key, value);
    }

    @Override
    public void close() {
        super.close();
        this.closed = true;
    }
}