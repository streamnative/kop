package io.streamnative.pulsar.handlers.kop;

import lombok.AllArgsConstructor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

@AllArgsConstructor
public class KafkaPositionImpl implements Position {

    protected long offset;
    protected long ledgerId;
    protected long entryId;

    public static final KafkaPositionImpl earliest = new KafkaPositionImpl(-1, -1, -1);
    public static final KafkaPositionImpl latest = new KafkaPositionImpl(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);

    public static KafkaPositionImpl get(long offset, long ledgerId, long entryId) {
        return new KafkaPositionImpl(offset, ledgerId, entryId);
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public Position getNext() {
        if (entryId < 0) {
            return PositionImpl.get(ledgerId, 0);
        } else {
            return PositionImpl.get(ledgerId, entryId + 1);
        }
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
