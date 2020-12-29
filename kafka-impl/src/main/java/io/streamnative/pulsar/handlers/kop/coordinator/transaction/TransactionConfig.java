package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

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
    public static final int DefaultTransactionMetadataTopicPartition = 1;
    public static final int DefaultTransactionMaxTimieoutMs = 1000 * 60 * 60 * 24;

    @Default
    private String transactionMetadataTopicName = DefaultTransactionMetadataTopicName;
    @Default
    private int transactionMetadataTopicPartition = DefaultTransactionMetadataTopicPartition;
    @Default
    private int transactionMaxTimeoutMs = DefaultTransactionMaxTimieoutMs;

}
