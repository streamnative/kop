package io.streamnative.pulsar.handlers.kop;

import lombok.AllArgsConstructor;
import org.apache.bookkeeper.mledger.Position;

@AllArgsConstructor
public class KafkaPositionImpl implements Position {

    protected long offset;
    protected long ledgerId;
    protected long entryId;

    public static KafkaPositionImpl get(long offset, long ledgerId, long entryId) {
        return new KafkaPositionImpl(offset, ledgerId, entryId);
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public Position getNext() {
        return null;
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }
}
