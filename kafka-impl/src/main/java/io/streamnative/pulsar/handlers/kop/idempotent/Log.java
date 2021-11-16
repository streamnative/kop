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
package io.streamnative.pulsar.handlers.kop.idempotent;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;

@Slf4j
@AllArgsConstructor
public class Log {

    private ProducerStateManager producerStateManager;

    /**
     * AppendOrigin is used mark the data origin.
     */
    public enum AppendOrigin {
        Coordinator,
        Client,
        Log
    }

    /**
     * CompletedTxn.
     */
    @ToString
    @AllArgsConstructor
    @Data
    public static class CompletedTxn {
        private Long producerId;
        private Long firstOffset;
        private Long lastOffset;
        private Boolean isAborted;
    }

    /**
     * Analyze result.
     */
    @Data
    @AllArgsConstructor
    public static class AnalyzeResult {
        private Map<Long, ProducerStateManager.ProducerAppendInfo> updatedProducers;
        private List<CompletedTxn> completedTxns;
        private Optional<ProducerStateManager.BatchMetadata> maybeDuplicate;

    }


    public AnalyzeResult analyzeAndValidateProducerState(MemoryRecords records,
                                                         Optional<Long> firstOffset,
                                                         AppendOrigin origin) {
        Map<Long, ProducerStateManager.ProducerAppendInfo> updatedProducers = Maps.newHashMap();
        List<Log.CompletedTxn> completedTxns = Lists.newArrayList();

        for (RecordBatch batch : records.batches()) {
            if (batch.hasProducerId()) {
                Optional<ProducerStateManager.ProducerStateEntry> maybeLastEntry =
                        producerStateManager.lastEntry(batch.producerId());

                // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
                // If we find a duplicate, we return the metadata of the appended batch to the client.
                if (maybeLastEntry.isPresent()) {
                    Optional<ProducerStateManager.BatchMetadata> maybeDuplicate =
                            maybeLastEntry.get().findDuplicateBatch(batch);
                    if (maybeDuplicate.isPresent()) {
                        return new AnalyzeResult(updatedProducers, completedTxns, maybeDuplicate);
                    }
                }
                // We cache offset metadata for the start of each transaction. This allows us to
                // compute the last stable offset without relying on additional index lookups.
                Optional<Log.CompletedTxn> maybeCompletedTxn =
                        updateProducers(batch, updatedProducers, firstOffset, origin);
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }
        }
        return new AnalyzeResult(updatedProducers, completedTxns, Optional.empty());
    }

    public void append(AnalyzeResult analyzeResult, long lastOffset) {
        analyzeResult.getUpdatedProducers().forEach((pid, appendInfo) -> {
            log.info("append pid: [{}], appendInfo: [{}], lastOffset: [{}]", pid, appendInfo, lastOffset);
            producerStateManager.update(appendInfo);
        });
        analyzeResult.getCompletedTxns().forEach(completedTxn -> {
            long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
            producerStateManager.updateTxnIndex(completedTxn, lastStableOffset);
            producerStateManager.completeTxn(completedTxn);
        });
        producerStateManager.updateMapEndOffset(lastOffset + 1);
    }

    public void appendTxnMarker(AnalyzeResult analyzeResult, long lastOffset) {
        analyzeResult.getUpdatedProducers().forEach((pid, appendInfo) -> {
            log.info("append pid: [{}], appendInfo: [{}], lastOffset: [{}]", pid, appendInfo, lastOffset);
            producerStateManager.update(appendInfo);
        });
        if (!analyzeResult.getCompletedTxns().isEmpty()) {
            Log.CompletedTxn completedTxn =
                    analyzeResult.getCompletedTxns().get(0);
            completedTxn.setLastOffset(lastOffset);
            producerStateManager.completeTxn(completedTxn);
        }
        producerStateManager.updateMapEndOffset(lastOffset + 1);
    }

    private Optional<Log.CompletedTxn> updateProducers(
            RecordBatch batch,
            Map<Long, ProducerStateManager.ProducerAppendInfo> producers,
            Optional<Long> firstOffset,
            AppendOrigin origin) {
        Long producerId = batch.producerId();
        ProducerStateManager.ProducerAppendInfo appendInfo =
                producers.computeIfAbsent(producerId, pid -> producerStateManager.prepareUpdate(producerId, origin));
        return appendInfo.append(batch, firstOffset);
    }

    public Optional<Long> firstUndecidedOffset() {
        Optional<Long> aLong = producerStateManager.firstUndecidedOffset();
        log.info("firstUndecidedOffset: {}", aLong);
        return aLong;
    }

    public List<FetchResponse.AbortedTransaction> getAbortedIndexList(long fetchOffset) {
        return producerStateManager.getAbortedIndexList(fetchOffset);
    }

    public void update(ProducerStateManager.ProducerAppendInfo appendInfo) {
        producerStateManager.update(appendInfo);
    }
}
