package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import lombok.Builder;
import lombok.Data;

/**
 * This class presents aborted index data.
 */
@Builder
@Data
public class AbortedIndexEntry {

    private short version;
    private long pid;
    private long firstOffset;
    private long lastOffset;
    private long lastStableOffset;

}
