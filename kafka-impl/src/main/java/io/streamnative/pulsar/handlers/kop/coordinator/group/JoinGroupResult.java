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
package io.streamnative.pulsar.handlers.kop.coordinator.group;

import java.util.Map;
import lombok.Data;
import org.apache.kafka.common.protocol.Errors;

/**
 * The result of a join group operation.
 */
@Data
public class JoinGroupResult {

    private final Map<String, byte[]> members;
    private final String memberId;
    private final int generationId;
    private final String protocolName;
    private final String protocolType;
    private final String leaderId;
    private final Errors error;

}
