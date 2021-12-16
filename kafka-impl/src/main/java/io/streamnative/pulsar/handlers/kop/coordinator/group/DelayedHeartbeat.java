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

import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import java.util.Optional;

/**
 * Delayed heartbeat operations that are added to the purgatory for session timeout checking.
 * Heartbeats are paused during rebalance.
 */
class DelayedHeartbeat extends DelayedOperation {

    private final GroupCoordinator coordinator;
    private final GroupMetadata group;
    private final MemberMetadata member;
    private long heartbeatDeadline;

    DelayedHeartbeat(GroupCoordinator coordinator,
                     GroupMetadata group,
                     MemberMetadata member,
                     long heartbeatDeadline,
                     long sessionTimeout) {
        super(sessionTimeout, Optional.of(group.lock()));

        this.coordinator = coordinator;
        this.group = group;
        this.member = member;
        this.heartbeatDeadline = heartbeatDeadline;
    }

    @Override
    public void onExpiration() {
        coordinator.onExpireHeartbeat(group, member, heartbeatDeadline);
    }

    @Override
    public void onComplete() {
        coordinator.onCompleteHeartbeat();
    }

    @Override
    public boolean tryComplete(boolean notify) {
        return coordinator.tryCompleteHeartbeat(group, member, heartbeatDeadline, () -> forceComplete());
    }

}
