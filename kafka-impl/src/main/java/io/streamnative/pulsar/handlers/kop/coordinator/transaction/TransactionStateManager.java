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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;

/**
 * Transaction state manager.
 */
@Slf4j
public class TransactionStateManager {

    private final TransactionConfig transactionConfig;
    private final Map<String, TransactionMetadata> transactionStateMap;
    private ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    public TransactionStateManager(TransactionConfig transactionConfig) {
        this.transactionConfig = transactionConfig;
        this.transactionStateMap = new ConcurrentHashMap<>();
    }

    public void putTransactionStateIfNotExists(String transactionalId, TransactionMetadata metadata) {
        transactionStateMap.put(transactionalId, metadata);
    }

    public void appendTransactionToLog(String transactionalId,
                                       int coordinatorEpoch,
                                       TransactionMetadata.TxnTransitMetadata newMetadata,
                                       ResponseCallback responseCallback) {
        try {
            // TODO save transaction log
            TransactionMetadata metadata = getTransactionState(transactionalId);
            metadata.completeTransitionTo(newMetadata);
            responseCallback.complete();
        } catch (Exception e) {
            // TODO exception process
            log.error("failed to handle", e);
            responseCallback.fail(e);
        }
    }

    /**
     * Response callback interface.
     */
    public interface ResponseCallback {
        void complete();
        void fail(Exception e);
    }

    public ByteBuf getWriteMarker(String transactionalId) {
        TransactionMetadata metadata = transactionStateMap.get(transactionalId);
        WriteTxnMarkersRequest.TxnMarkerEntry txnMarkerEntry = new WriteTxnMarkersRequest.TxnMarkerEntry(
                metadata.getProducerId(),
                metadata.getProducerEpoch(),
                1,
                TransactionResult.COMMIT,
                new ArrayList<>(metadata.getTopicPartitions()));
        WriteTxnMarkersRequest txnMarkersRequest = new WriteTxnMarkersRequest.Builder(
                Lists.newArrayList(txnMarkerEntry)).build();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, txnMarkersRequest.version(), "", -1);
        return RequestUtils.serializeRequest(txnMarkersRequest.version(), requestHeader, txnMarkersRequest);
    }


    public TransactionMetadata getTransactionState(String transactionalId) {
        return transactionStateMap.get(transactionalId);
    }

    /**
     * Validate the given transaction timeout value.
     */
    public boolean validateTransactionTimeoutMs(int txnTimeoutMs) {
        return txnTimeoutMs <= transactionConfig.getTransactionMaxTimeoutMs() && txnTimeoutMs > 0;
    }

}
