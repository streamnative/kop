package io.streamnative.pulsar.handlers.kop.storage.snapshot;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
@AllArgsConstructor
public class AbortedTxnEntry {
    private final Long producerId;
    private final Long firstOffset;
    private final Long lastOffset;
    private final Long lastStableOffset;
}
