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
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance.
 *
 * <p>Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * <p>When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 */
class DelayedJoin extends DelayedOperation {

    final GroupCoordinator coordinator;
    final GroupMetadata group;

    protected DelayedJoin(GroupCoordinator coordinator,
                          GroupMetadata group,
                          long rebalanceTimeout) {
        super(rebalanceTimeout, Optional.of(group.lock()));
        this.coordinator = coordinator;
        this.group = group;
    }

    @Override
    public void onExpiration() {
        coordinator.onExpireJoin();
    }

    @Override
    public void onComplete() {
        coordinator.onCompleteJoin(group);
    }

    @Override
    public boolean tryComplete() {
        return coordinator.tryCompleteJoin(group, () -> forceComplete());
    }

}
