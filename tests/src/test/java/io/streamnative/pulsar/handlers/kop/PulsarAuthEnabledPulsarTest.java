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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * {@link PulsarAuthEnabledTestBase} with `entryFormat=pulsar`.
 */
public class PulsarAuthEnabledPulsarTest extends PulsarAuthEnabledTestBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.setup();
    }
    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.cleanup();
    }

    public PulsarAuthEnabledPulsarTest() {
        super("pulsar");
    }
}
