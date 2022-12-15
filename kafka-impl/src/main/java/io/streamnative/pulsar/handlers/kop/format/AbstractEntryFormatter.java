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
package io.streamnative.pulsar.handlers.kop.format;

import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.kop.exceptions.MetadataCorruptedException;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;

@Slf4j
public abstract class AbstractEntryFormatter implements EntryFormatter {

    // These key-value identifies the entry's format as kafka
    public static final String IDENTITY_KEY = "entry.format";
    public static final String IDENTITY_VALUE = EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase();
    private final Time time = Time.SYSTEM;
    private final ImmutableList<EntryFilterWithClassLoader> entryfilters;

    private final boolean applyAvroSchemaOnDecode;

    protected AbstractEntryFormatter(ImmutableList<EntryFilterWithClassLoader> entryfilters,
                                     boolean applyAvroSchemaOnDecode) {
        this.entryfilters = entryfilters;
        this.applyAvroSchemaOnDecode = applyAvroSchemaOnDecode;
    }

    @Override
    public final CompletableFuture<DecodeResult> decode(List<Entry> entries, byte magic,
                                     String pulsarTopicName, SchemaManager schemaManager) {

        if (!applyAvroSchemaOnDecode) {
            return CompletableFuture.completedFuture(decodeSync(entries, magic, pulsarTopicName, schemaManager));
        } else {
            return decodeAsync(entries, magic, pulsarTopicName, schemaManager);
        }
    }

    /**
     * Async decode the entries and apply the schema.
     * @param entries
     * @param magic
     * @param pulsarTopicName
     * @param schemaManager
     * @return
     */
    private CompletableFuture<DecodeResult> decodeAsync(List<Entry> entries, byte magic,
                                                        String pulsarTopicName, SchemaManager schemaManager) {
        AtomicInteger totalSize = new AtomicInteger();
        AtomicInteger conversionCount = new AtomicInteger();
        AtomicLong conversionTimeNanos = new AtomicLong();
        // batched ByteBuf should be released after sending to client
        ByteBuf batchedByteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer();
        CompletableFuture<DecodeResult> result = new CompletableFuture<>();

        processEntry(entries, 0, magic, pulsarTopicName, schemaManager, totalSize, conversionCount, conversionTimeNanos,
                    batchedByteBuf, result);

        return result;
    }

    private void processEntry(List<Entry> entries, int index, byte magic, String pulsarTopicName,
                              SchemaManager schemaManager, AtomicInteger totalSize,
                              AtomicInteger conversionCount, AtomicLong conversionTimeNanos, ByteBuf batchedByteBuf,
                              CompletableFuture<DecodeResult> result) {
        if (index == entries.size()) {
            result.complete(DecodeResult.get(
                    MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(batchedByteBuf)),
                    batchedByteBuf,
                    conversionCount.get(),
                    conversionTimeNanos.get()));
            return;
        }
        final Entry entry = entries.get(index);
        final long startOffset;
        final ByteBuf byteBuf;
        final MessageMetadata metadata;

        try {
            startOffset = MessageMetadataUtils.peekBaseOffsetFromEntry(entry);
            byteBuf = entry.getDataBuffer();
            metadata = MessageMetadataUtils.parseMessageMetadata(byteBuf);

            // Almost all exceptions in Kafka inherit from KafkaException and will be captured
            // and processed in KafkaApis. Here, whether it is down-conversion or the IOException
            // in builder.appendWithOffset in decodePulsarEntryToKafkaRecords will be caught by Kafka
            // and the KafkaException will be thrown. So we need to catch KafkaException here.
        } catch (MetadataCorruptedException | KafkaException e) { // skip failed decode entry
            entry.release();

            // next entry
            log.error("[{}:{}] Failed to decode entry. skipping.", entry.getLedgerId(), entry.getEntryId(), e);
            processEntry(entries, index + 1, magic,
                    pulsarTopicName, schemaManager, totalSize,
                    conversionCount, conversionTimeNanos, batchedByteBuf,
                    result);
            return;
        } catch (RuntimeException e) {
            entry.release();
            log.error("[{}:{}] Fatal error while decoding entry. ", entry.getLedgerId(), entry.getEntryId(), e);
            result.completeExceptionally(e);
            return;
        }

        if (isKafkaEntryFormat(metadata)) {
            decodeKafkaEntry(magic, totalSize, conversionCount, conversionTimeNanos, batchedByteBuf, entry,
                    startOffset, byteBuf);
            entry.release();
            // next entry (same thread)
            processEntry(entries, index + 1, magic,
                    pulsarTopicName, schemaManager, totalSize,
                    conversionCount, conversionTimeNanos, batchedByteBuf,
                    result);
        } else {
            // handle schema, this MAY require some async operations
            // the entry MAY be processed in a different thread
            ByteBufUtils.decodePulsarEntryToKafkaRecordsAsync(pulsarTopicName,
                            metadata, byteBuf, startOffset,
                            magic, schemaManager)
                    .thenAccept((DecodeResult decodeResult) -> {

                decodePulsarEntry(decodeResult, totalSize, conversionCount, conversionTimeNanos, batchedByteBuf);

                // next entry
                processEntry(entries, index + 1, magic,
                                pulsarTopicName, schemaManager, totalSize,
                                conversionCount, conversionTimeNanos, batchedByteBuf,
                                result);
            }).exceptionally(err -> {
                log.error("Error while decoding entry", err);
                // see below
                if ((err.getCause() instanceof MetadataCorruptedException)
                    || (err.getCause() instanceof IOException)
                        || (err.getCause() instanceof KafkaException)) {
                    // next entry
                    processEntry(entries, index + 1, magic,
                            pulsarTopicName, schemaManager, totalSize,
                            conversionCount, conversionTimeNanos, batchedByteBuf,
                            result);
                } else {
                    result.completeExceptionally(err);
                }
                return null;
            }).whenComplete((___, err) -> {
               entry.release();
            });
        }

    }

    private DecodeResult decodeSync(List<Entry> entries, byte magic,
                                         String pulsarTopicName, SchemaManager schemaManager) {
        AtomicInteger totalSize = new AtomicInteger();
        AtomicInteger conversionCount = new AtomicInteger();
        AtomicLong conversionTimeNanos = new AtomicLong();
        // batched ByteBuf should be released after sending to client
        ByteBuf batchedByteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer();
        for (Entry entry : entries) {
            try {
                long startOffset = MessageMetadataUtils.peekBaseOffsetFromEntry(entry);
                final ByteBuf byteBuf = entry.getDataBuffer();
                final MessageMetadata metadata = MessageMetadataUtils.parseMessageMetadata(byteBuf);
                EntryFilter.FilterResult filterResult = filterOnlyByMsgMetadata(metadata, entry, entryfilters);
                if (filterResult == EntryFilter.FilterResult.REJECT) {
                    continue;
                }
                if (isKafkaEntryFormat(metadata)) {
                    decodeKafkaEntry(magic, totalSize, conversionCount, conversionTimeNanos, batchedByteBuf, entry,
                            startOffset, byteBuf);
                } else {
                    final DecodeResult decodeResult =
                            ByteBufUtils.decodePulsarEntryToKafkaRecords(pulsarTopicName,
                                    metadata, byteBuf, startOffset,
                                    magic, schemaManager, false);
                    decodePulsarEntry(decodeResult, totalSize, conversionCount, conversionTimeNanos, batchedByteBuf);
                }

                // Almost all exceptions in Kafka inherit from KafkaException and will be captured
                // and processed in KafkaApis. Here, whether it is down-conversion or the IOException
                // in builder.appendWithOffset in decodePulsarEntryToKafkaRecords will be caught by Kafka
                // and the KafkaException will be thrown. So we need to catch KafkaException here.
            } catch (MetadataCorruptedException | IOException | KafkaException e) { // skip failed decode entry
                log.error("[{}:{}] Failed to decode entry. ", entry.getLedgerId(), entry.getEntryId(), e);
            } finally {
                entry.release();
            }
        }

        return DecodeResult.get(
                MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(batchedByteBuf)),
                batchedByteBuf,
                conversionCount.get(),
                conversionTimeNanos.get());
    }

    private void decodePulsarEntry(DecodeResult decodeResult, AtomicInteger totalSize, AtomicInteger conversionCount,
                                   AtomicLong conversionTimeNanos, ByteBuf batchedByteBuf) {
        conversionCount.addAndGet(decodeResult.getConversionCount());
        conversionTimeNanos.addAndGet(decodeResult.getConversionTimeNanos());
        final ByteBuf kafkaBuffer = decodeResult.getOrCreateByteBuf();
        totalSize.addAndGet(kafkaBuffer.readableBytes());
        batchedByteBuf.writeBytes(kafkaBuffer);
        decodeResult.recycle();
    }

    private void decodeKafkaEntry(byte magic, AtomicInteger totalSize, AtomicInteger conversionCount,
                           AtomicLong conversionTimeNanos, ByteBuf batchedByteBuf, Entry entry, long startOffset,
                           ByteBuf byteBuf) {
        byte batchMagic = byteBuf.getByte(byteBuf.readerIndex() + MAGIC_OFFSET);
        byteBuf.setLong(byteBuf.readerIndex() + OFFSET_OFFSET, startOffset);

        // batch magic greater than the magic corresponding to the version requested by the client
        // need down converted
        if (batchMagic > magic) {
            long startConversionNanos = MathUtils.nowInNano();
            MemoryRecords memoryRecords = MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(byteBuf));
            // down converted, batch magic will be set to client magic
            ConvertedRecords<MemoryRecords> convertedRecords =
                    memoryRecords.downConvert(magic, startOffset, time);
            conversionCount.addAndGet(convertedRecords.recordConversionStats().numRecordsConverted());
            conversionTimeNanos.addAndGet(MathUtils.elapsedNanos(startConversionNanos));

            final ByteBuf kafkaBuffer = Unpooled.wrappedBuffer(convertedRecords.records().buffer());
            totalSize.addAndGet(kafkaBuffer.readableBytes());
            batchedByteBuf.writeBytes(kafkaBuffer);
            kafkaBuffer.release();
            if (log.isTraceEnabled()) {
                log.trace("[{}:{}] MemoryRecords down converted, start offset {},"
                                + " entry magic: {}, client magic: {}",
                        entry.getLedgerId(), entry.getEntryId(), startOffset, batchMagic, magic);
            }

        } else {
            // not need down converted, batch magic retains the magic value written in production
            ByteBuf buf = byteBuf.slice(byteBuf.readerIndex(), byteBuf.readableBytes());
            totalSize.addAndGet(buf.readableBytes());
            batchedByteBuf.writeBytes(buf);
        }
    }

    protected static boolean isKafkaEntryFormat(final MessageMetadata messageMetadata) {
        final List<KeyValue> keyValues = messageMetadata.getPropertiesList();
        for (KeyValue keyValue : keyValues) {
            if (keyValue.hasKey()
                    && keyValue.getKey().equals(IDENTITY_KEY)
                    && keyValue.getValue().equals(IDENTITY_VALUE)) {
                return true;
            }
        }
        return false;
    }

    protected EntryFilter.FilterResult filterOnlyByMsgMetadata(MessageMetadata msgMetadata, Entry entry,
                                                                    List<EntryFilterWithClassLoader> entryFilters) {
        if (entryFilters == null || entryFilters.isEmpty()) {
            return EntryFilter.FilterResult.ACCEPT;
        }
        FilterContext filterContext = new FilterContext();
        filterContext.setMsgMetadata(msgMetadata);

        EntryFilter.FilterResult result = EntryFilter.FilterResult.ACCEPT;
        for (EntryFilter entryFilter : entryFilters) {
            EntryFilter.FilterResult filterResult = entryFilter.filterEntry(entry, filterContext);
            if (filterResult == null
                    || filterResult == EntryFilter.FilterResult.RESCHEDULE
                    || filterResult == EntryFilter.FilterResult.ACCEPT) {
                continue;
            }
            if (filterResult == EntryFilter.FilterResult.REJECT) {
                result = EntryFilter.FilterResult.REJECT;
                break;
            }
        }
        return result;
    }
}
