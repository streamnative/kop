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

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey.GroupKey;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;

/**
 * Delayed rebalance operation that is added to the purgatory when a group is transitioning from
 * Empty to PreparingRebalance
 *
 * <p>When onComplete is triggered we check if any new members have been added and if there is still time remaining
 * before the rebalance timeout. If both are true we then schedule a further delay. Otherwise we complete the
 * rebalance.
 */
class InitialDelayedJoin extends DelayedJoin {

    final DelayedOperationPurgatory<DelayedJoin> purgatory;
    final int configuredRebalanceDelay;
    final int delayMs;
    final int remainingMs;

    InitialDelayedJoin(GroupCoordinator coordinator,
                       DelayedOperationPurgatory<DelayedJoin> purgatory,
                       GroupMetadata group,
                       int configuredRebalanceDelay,
                       int delayMs,
                       int remainingMs) {
        super(coordinator, group, delayMs);
        this.purgatory = purgatory;
        this.configuredRebalanceDelay = configuredRebalanceDelay;
        this.delayMs = delayMs;
        this.remainingMs = remainingMs;
    }

    @Override
    public void onComplete() {
        group.inLock(() -> {
            if (group.newMemberAdded() && remainingMs != 0) {
                group.newMemberAdded(false);
                int delay = Math.min(configuredRebalanceDelay, remainingMs);
                int remaining = Math.max(
                    remainingMs - delayMs,
                    0
                );
                purgatory.tryCompleteElseWatch(
                    new InitialDelayedJoin(
                        coordinator,
                        purgatory,
                        group,
                        configuredRebalanceDelay,
                        delay,
                        remaining
                    ),
                    Lists.newArrayList(new GroupKey(group.groupId()))
                );
            } else {
                super.onComplete();
            }
            return null;
        });
    }

    @Override
    public boolean tryComplete() {
        return false;
    }

}
