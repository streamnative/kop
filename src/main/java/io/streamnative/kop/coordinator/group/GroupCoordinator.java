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
package io.streamnative.kop.coordinator.group;

import io.streamnative.kop.coordinator.group.GroupMetadata.GroupSummary;
import java.util.Collections;

/**
 * Group coordinator.
 */
public class GroupCoordinator {

    static final String NoState = "";
    static final String NoProtocolType = "";
    static final String NoProtocol = "";
    static final String NoLeader = "";
    static final int NoGeneration = -1;
    static final String NoMemberId = "";
    static final GroupSummary EmptyGroup = new GroupSummary(
        NoState,
        NoProtocolType,
        NoProtocol,
        Collections.emptyList()
    );
    static final GroupSummary DeadGroup = new GroupSummary(
        GroupState.Dead.toString(),
        NoProtocolType,
        NoProtocol,
        Collections.emptyList()
    );


}
