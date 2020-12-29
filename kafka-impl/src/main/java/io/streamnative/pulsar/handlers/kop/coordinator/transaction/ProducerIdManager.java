package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class used to manage producer id.
 */
public class ProducerIdManager {

    private final AtomicLong producerId = new AtomicLong(0);

    public long generateProducerId() {
        // TODO generate unique producer id
        return producerId.incrementAndGet();
    }

}
