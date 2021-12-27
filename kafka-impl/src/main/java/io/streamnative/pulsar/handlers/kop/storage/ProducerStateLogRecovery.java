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
package io.streamnative.pulsar.handlers.kop.storage;

import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.kafka.common.record.RecordBatch;


/**
 * ProducerStateLogRecovery is used to recover producer state from logs.
 */
@Slf4j
public class ProducerStateLogRecovery {

    private final PartitionLog partitionLog;
    private final EntryFormatter entryFormatter;
    private final ManagedCursor cursor;
    private int cacheQueueSize;
    private final List<Entry> readEntryList = new ArrayList<>();
    private int maxErrorCount = 10;
    private int errorCount = 0;
    private boolean readComplete = false;
    private boolean havePendingRead = false;
    private boolean recoverComplete = false;
    private boolean recoverError = false;

    public ProducerStateLogRecovery(PartitionLog partitionLog,
                                     EntryFormatter entryFormatter,
                                     ManagedCursor cursor,
                                     int cacheQueueSize) {
        this.partitionLog = partitionLog;
        this.entryFormatter = entryFormatter;
        this.cursor = cursor;
        this.cacheQueueSize = cacheQueueSize;
    }

    private void fillCacheQueue() {
        havePendingRead = true;
        cursor.asyncReadEntries(cacheQueueSize, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                havePendingRead = false;
                if (entries.size() == 0) {
                    log.info("Can't read more entries, finish to recover topic.");
                    readComplete = true;
                    return;
                }
                readEntryList.addAll(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                havePendingRead = false;
                if (exception instanceof ManagedLedgerException.NoMoreEntriesToReadException) {
                    log.info("No more entries to read, finish to recover topic.");
                    readComplete = true;
                    return;
                }
                checkErrorCount(exception);
            }
        }, null, null);
    }

    protected void recover() {
        while (!recoverComplete && !recoverError && readEntryList.size() > 0) {
            if (!havePendingRead && !readComplete) {
                fillCacheQueue();
            }
            if (readEntryList.size() > 0) {
                List<Entry> entryList = new ArrayList<>(readEntryList);
                readEntryList.clear();
                fillCacheQueue();
                DecodeResult decodeResult = entryFormatter.decode(entryList, RecordBatch.CURRENT_MAGIC_VALUE);
                Map<Long, ProducerAppendInfo> appendInfoMap = new HashMap<>();
                List<CompletedTxn> completedTxns = new ArrayList<>();
                decodeResult.getRecords().batches().forEach(batch -> {
                    Optional<CompletedTxn> completedTxn =
                            partitionLog.updateProducers(batch,
                                    appendInfoMap,
                                    Optional.empty(),
                                    PartitionLog.AppendOrigin.Log);
                    completedTxn.ifPresent(completedTxns::add);
                });
                appendInfoMap.values().forEach(partitionLog::updateProducerAppendInfo);
                completedTxns.forEach(partitionLog::completeTxn);
                if (readComplete) {
                    recoverComplete = true;
                }
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    checkErrorCount(e);
                }
            }
        }
        log.info("Finish to recover from logs.");
    }

    private void checkErrorCount(Throwable throwable) {
        if (errorCount < maxErrorCount) {
            errorCount++;
            log.error("[{}] Recover error count {}. msg: {}.", errorCount, throwable.getMessage(), throwable);
        } else {
            recoverError = true;
            log.error("Failed to recover.");
        }
    }

}
