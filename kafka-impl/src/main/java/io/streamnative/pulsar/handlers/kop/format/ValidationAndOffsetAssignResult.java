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
package io.streamnative.pulsar.handlers.kop.format;

import io.netty.util.Recycler;
import lombok.Getter;
import org.apache.kafka.common.record.MemoryRecords;

/**
 * Result of KopLogValidator validateMessagesAndAssignOffsets in KafkaMixedEntryFormatter.
 */
@Getter
public class ValidationAndOffsetAssignResult {

    private MemoryRecords records;
    private int conversionCount;
    private long conversionTimeNanos;

    private final Recycler.Handle<ValidationAndOffsetAssignResult> recyclerHandle;

    public static ValidationAndOffsetAssignResult get(MemoryRecords records,
                                                      int conversionCount,
                                                      long conversionTimeNanos) {
        ValidationAndOffsetAssignResult validationAndOffsetAssignResult = RECYCLER.get();
        validationAndOffsetAssignResult.records = records;
        validationAndOffsetAssignResult.conversionCount = conversionCount;
        validationAndOffsetAssignResult.conversionTimeNanos = conversionTimeNanos;
        return validationAndOffsetAssignResult;
    }

    private ValidationAndOffsetAssignResult(Recycler.Handle<ValidationAndOffsetAssignResult> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<ValidationAndOffsetAssignResult> RECYCLER =
            new Recycler<ValidationAndOffsetAssignResult>() {
                @Override
                protected ValidationAndOffsetAssignResult newObject(
                        Recycler.Handle<ValidationAndOffsetAssignResult> handle) {
                    return new ValidationAndOffsetAssignResult(handle);
                }
    };

    public void recycle() {
        records = null;
        conversionCount = -1;
        conversionTimeNanos = -1L;
        recyclerHandle.recycle(this);
    }

}
