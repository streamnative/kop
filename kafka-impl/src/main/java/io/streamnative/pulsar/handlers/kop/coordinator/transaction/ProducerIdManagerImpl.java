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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds in a unique way
 * such that the same producerId will not be assigned twice across multiple transaction coordinators.
 *
 * ProducerIds are managed via ZooKeeper, where the latest producerId block is written on the corresponding ZK
 * path by the manager who claims the block, where the written block_start and block_end are both inclusive.
 */
@Slf4j
public class ProducerIdManagerImpl implements ProducerIdManager {

    private static final Long currentVersion = 1L;
    public static final Long PID_BLOCK_SIZE = 1000L;
    public static final String KOP_PID_BLOCK_ZNODE = "/kop_latest_producer_id_block";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final int brokerId;
    private final MetadataStoreExtended metadataStore;

    private ProducerIdBlock currentProducerIdBlock;
    private Long nextProducerId = -1L;

    private CompletableFuture<Void> newProducerIdBlockFuture;

    public ProducerIdManagerImpl(int brokerId, MetadataStoreExtended metadataStore) {
        this.brokerId = brokerId;
        this.metadataStore = metadataStore;
    }

    public static byte[] generateProducerIdBlockJson(ProducerIdBlock producerIdBlock) throws
            JsonProcessingException {
        Map<String, Object> dataMap = Maps.newHashMap();
        dataMap.put("version", currentVersion);
        dataMap.put("broker", producerIdBlock.brokerId);
        dataMap.put("block_start", producerIdBlock.blockStartId);
        dataMap.put("block_end", producerIdBlock.blockEndId);
        return objectMapper.writeValueAsBytes(dataMap);
    }

    public static ProducerIdBlock parseProducerIdBlockData(byte[] bytes) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(bytes);
        return ProducerIdBlock.builder()
                .brokerId(jsonNode.get("broker").asInt())
                .blockStartId(jsonNode.get("block_start").asLong())
                .blockEndId(jsonNode.get("block_end").asLong())
                .build();
    }

    public synchronized CompletableFuture<Void> getNewProducerIdBlock() {
        if (newProducerIdBlockFuture != null && !newProducerIdBlockFuture.isDone()) {
            // In this case, the class is already getting the new producer id block.
            // Returning this future ensures that callbacks work correctly
            return newProducerIdBlockFuture;
        }
        newProducerIdBlockFuture = new CompletableFuture<>();
        getCurrentDataAndVersion().thenAccept(currentDataAndVersionOpt -> {
            synchronized (this) {
                final ProducerIdBlock nextProducerIdBlock;
                if (currentDataAndVersionOpt.isPresent() && currentDataAndVersionOpt.get().getData() != null) {
                    DataAndVersion dataAndVersion = currentDataAndVersionOpt.get();
                    try {
                        ProducerIdBlock currProducerIdBlock =
                                ProducerIdManagerImpl.parseProducerIdBlockData(dataAndVersion.getData());
                        if (currProducerIdBlock.blockEndId > Long.MAX_VALUE - ProducerIdManagerImpl.PID_BLOCK_SIZE) {
                            // We have exhausted all producerIds (wow!), treat it as a fatal error
                            log.error("Exhausted all producerIds as the next block's end producerId is will "
                                            + "has exceeded long type limit (current block end producerId is {})",
                                    currProducerIdBlock.blockEndId);
                            newProducerIdBlockFuture
                                    .completeExceptionally(new KafkaException("Have exhausted all producerIds."));
                            return;
                        }
                        nextProducerIdBlock = ProducerIdBlock
                                .builder()
                                .brokerId(brokerId)
                                .blockStartId(currProducerIdBlock.blockEndId + 1L)
                                .blockEndId(currProducerIdBlock.blockEndId + ProducerIdManagerImpl.PID_BLOCK_SIZE)
                                .build();
                    } catch (IOException e) {
                        newProducerIdBlockFuture.completeExceptionally(new KafkaException("Get producerId failed.", e));
                        return;
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("There is no producerId block yet, creating the first block");
                    }
                    nextProducerIdBlock = ProducerIdBlock
                            .builder()
                            .brokerId(brokerId)
                            .blockStartId(0L)
                            .blockEndId(ProducerIdManagerImpl.PID_BLOCK_SIZE - 1)
                            .build();
                }
                try {
                    byte[] newProducerIdBlockData = ProducerIdManagerImpl
                            .generateProducerIdBlockJson(nextProducerIdBlock);
                    conditionalUpdateData(newProducerIdBlockData,
                            currentDataAndVersionOpt.orElse(DataAndVersion.DEFAULT_VERSION).getVersion())
                            .thenAccept(version -> {
                                synchronized (this) {
                                    currentProducerIdBlock = nextProducerIdBlock;
                                    nextProducerId = nextProducerIdBlock.blockStartId;
                                    newProducerIdBlockFuture.complete(null);
                                }
                            }).exceptionally(ex -> {
                                synchronized (this) {
                                    newProducerIdBlockFuture.completeExceptionally(ex);
                                }
                                return null;
                            });
                } catch (JsonProcessingException e) {
                    newProducerIdBlockFuture.completeExceptionally(e);
                }
        }}).exceptionally(ex -> {
            synchronized (this) {
                newProducerIdBlockFuture.completeExceptionally(ex);
            }
            return null;
        });
        return newProducerIdBlockFuture;
    }

    @Override
    public CompletableFuture<Void> initialize() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        getNewProducerIdBlock()
                .thenAccept(__ -> {
                    synchronized (this) {
                        nextProducerId = currentProducerIdBlock.blockStartId;
                    }
                    future.complete(null);
                }).exceptionally(throwable -> {
                    future.completeExceptionally(throwable);
                    return null;
                });
        return future;
    }

    @Override
    public synchronized CompletableFuture<Long> generateProducerId() {
        CompletableFuture<Long> nextProducerIdFuture = new CompletableFuture<>();
        // grab a new block of producerIds if this block has been exhausted
        if (nextProducerId > currentProducerIdBlock.blockEndId) {
            getNewProducerIdBlock().thenAccept(__ -> {
                synchronized (this) {
                    if (nextProducerId > currentProducerIdBlock.blockEndId) {
                        // This can only happen if more than blockSize producers attempt to connect
                        // while the getNewProducerIdBlock() is processing
                        Exception ex = new IllegalStateException("New ProducerIdBlock exhausted. Try again.");
                        nextProducerIdFuture.completeExceptionally(ex);
                    } else {
                        nextProducerId += 1;
                        nextProducerIdFuture.complete(nextProducerId - 1);
                    }
                }
            }).exceptionally(ex -> {
                nextProducerIdFuture.completeExceptionally(ex);
                return null;
            });
        } else {
            nextProducerId += 1;
            nextProducerIdFuture.complete(nextProducerId - 1);
        }

        return nextProducerIdFuture;
    }

    private CompletableFuture<Long> conditionalUpdateData(byte[] data, long expectVersion) {
        CompletableFuture<Long> updateFuture = new CompletableFuture<>();
        metadataStore.put(KOP_PID_BLOCK_ZNODE, data, Optional.of(expectVersion))
                .thenAccept(stat -> {
                    if (log.isDebugEnabled()) {
                        log.debug("ConditionalUpdateData Expect version: {}, stat version: {}",
                                expectVersion, stat.getVersion());
                    }
                    updateFuture.complete(stat.getVersion());
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof MetadataStoreException.BadVersionException) {
                        checkProducerIdBlockMetadata(data)
                                .thenAccept(updateFuture::complete).exceptionally(e -> {
                                    updateFuture.completeExceptionally(e);
                                    return null;
                                });
                    } else if (ex.getCause() instanceof MetadataStoreException.NotFoundException) {
                        log.error("Update of path {} with data {} and expected version {} failed due to {}",
                                KOP_PID_BLOCK_ZNODE, getProducerIdBlockStr(data), expectVersion,
                                "NoNode for path " + KOP_PID_BLOCK_ZNODE);
                        updateFuture.completeExceptionally(new Exception("NoNode for path " + KOP_PID_BLOCK_ZNODE));
                    } else {
                        log.error("Update of path {} with data {} and expected version {} exception: {}",
                                KOP_PID_BLOCK_ZNODE, getProducerIdBlockStr(data), expectVersion, ex.getCause());
                        updateFuture.completeExceptionally(new Exception("Error to update data.", ex.getCause()));
                    }
                    return null;
                });
        return updateFuture;
    }

    private CompletableFuture<Long> checkProducerIdBlockMetadata(byte[] expectedData) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            ProducerIdBlock expectedPidBlock = ProducerIdManagerImpl.parseProducerIdBlockData(expectedData);
            getCurrentDataAndVersion().thenAccept(dataAndVersionOpt -> {
                if (dataAndVersionOpt.isPresent()) {
                    DataAndVersion dataAndVersion = dataAndVersionOpt.get();
                    byte[] data = dataAndVersion.getData();
                    try {
                        ProducerIdBlock producerIdBlock =
                                ProducerIdManagerImpl.parseProducerIdBlockData(data);
                        if (expectedPidBlock.equals(producerIdBlock)) {
                            long version = dataAndVersion.getVersion();
                            future.complete(version);
                            return;
                        }
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                        return;
                    }
                    future.complete(dataAndVersionOpt.get().getVersion());
                } else {
                    future.completeExceptionally(new Exception("ProducerId is not present !"));
                }
            }).exceptionally(ex -> {
                future.completeExceptionally(ex);
                return null;
            });
        } catch (IOException e) {
            log.warn("Error while checking for producerId block Zk data on path {}: expected data {}",
                    ProducerIdManagerImpl.KOP_PID_BLOCK_ZNODE, getProducerIdBlockStr(expectedData), e);
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<Optional<DataAndVersion>> getCurrentDataAndVersion() {
        CompletableFuture<Optional<DataAndVersion>> future = new CompletableFuture<>();
        metadataStore.get(KOP_PID_BLOCK_ZNODE)
                .thenAccept(resultOpt -> {
                    if (resultOpt.isPresent()) {
                        GetResult getResult = resultOpt.get();
                        future.complete(Optional.of(
                                new DataAndVersion(getResult.getValue(), getResult.getStat().getVersion())));
                    } else {
                        future.complete(Optional.empty());
                    }
                }).exceptionally(ex -> {
                    future.completeExceptionally(ex);
                    return null;
                });
        return future;
    }

    @Data
    @AllArgsConstructor
    private static class DataAndVersion {
        private byte[] data;
        private long version;

        public static final DataAndVersion DEFAULT_VERSION = new DataAndVersion(null, -1);
    }

    /**
     * ProducerId block.
     */
    @Builder
    @Data
    @AllArgsConstructor
    @ToString
    public static class ProducerIdBlock {
        private Integer brokerId;
        private Long blockStartId;
        private Long blockEndId;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProducerIdBlock that = (ProducerIdBlock) o;
            return Objects.equals(brokerId, that.brokerId)
                    && Objects.equals(blockStartId, that.blockStartId)
                    && Objects.equals(blockEndId, that.blockEndId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerId, blockStartId, blockEndId);
        }
    }

    private String getProducerIdBlockStr(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void shutdown() {
        log.info("Shutdown complete: last producerId assigned {}", nextProducerId);
    }

}
