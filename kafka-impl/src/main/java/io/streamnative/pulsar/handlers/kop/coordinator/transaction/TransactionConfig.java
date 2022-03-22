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

import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

/**
 * Transaction config.
 */
@Builder
@Data
public class TransactionConfig {

    public static final String DefaultTransactionMetadataTopicName = "public/default/__transaction_state";
    public static final String DefaultProducerIdTopicName = "public/default/__transaction_producerid_generator";
    public static final long DefaultTransactionsMaxTimeoutMs = TimeUnit.MINUTES.toMillis(15);
    public static final long DefaultTransactionalIdExpirationMs = TimeUnit.DAYS.toMillis(7);
    public static final long DefaultAbortTimedOutTransactionsIntervalMs = TimeUnit.SECONDS.toMillis(10);
    public static final long DefaultRemoveExpiredTransactionalIdsIntervalMs = TimeUnit.HOURS.toMillis(1);
    public static final int DefaultTransactionCoordinatorSchedulerNum = 1;
    public static final int DefaultTransactionStateManagerSchedulerNum = 1;
    public static final int DefaultTransactionLogNumPartitions = 8;

    @Default
    private int brokerId = 1;
    @Default
    private String transactionProducerIdTopicName = DefaultProducerIdTopicName;
    @Default
    private String transactionMetadataTopicName = DefaultTransactionMetadataTopicName;
    @Default
    private long transactionMaxTimeoutMs = DefaultTransactionsMaxTimeoutMs;
    @Default
    private long transactionalIdExpirationMs = DefaultTransactionalIdExpirationMs;
    @Default
    private int transactionLogNumPartitions = DefaultTransactionLogNumPartitions;
    @Default
    private long abortTimedOutTransactionsIntervalMs = DefaultAbortTimedOutTransactionsIntervalMs;
    @Default
    private long removeExpiredTransactionalIdsIntervalMs = DefaultRemoveExpiredTransactionalIdsIntervalMs;
    @Default
    private long requestTimeoutMs = 30000;
    @Default
    private int transactionCoordinatorSchedulerNum = DefaultTransactionCoordinatorSchedulerNum;
    @Default
    private int transactionStateManagerSchedulerNum = DefaultTransactionStateManagerSchedulerNum;

}
