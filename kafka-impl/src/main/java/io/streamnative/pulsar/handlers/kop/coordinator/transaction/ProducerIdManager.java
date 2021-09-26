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
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * This class used to manage producer id.
 */
@Slf4j
public class ProducerIdManager {

    private static final Long currentVersion = 1L;
    public static final Long PID_BLOCK_SIZE = 1000L;
    public static final String KOP_PID_BLOCK_ZNODE = "/kop_latest_producer_id_block";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Integer brokerId;
    private final ZooKeeper zkClient;

    private ProducerIdBlock currentProducerIdBlock;
    private Long nextProducerId = -1L;

    public ProducerIdManager(Integer brokerId, ZooKeeper zkClient) {
        this.brokerId = brokerId;
        this.zkClient = zkClient;
    }

    public static byte[] generateProducerIdBlockJson(ProducerIdBlock producerIdBlock) throws JsonProcessingException {
        Map<String, Object> dataMap = new HashMap<>();
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

    private CompletableFuture<Void> getNewProducerIdBlock() {
        final CompletableFuture<Void> setDataFuture = new CompletableFuture<>();

        // refresh current producerId block from zookeeper again
        getDataAndVersion().whenComplete((dataAndVersion, getDataThrowable) -> {
            if (getDataThrowable != null) {
                setDataFuture.completeExceptionally(getDataThrowable);
                return;
            }
            if (dataAndVersion == null) {
                currentProducerIdBlock = new ProducerIdBlock(brokerId, 0L, PID_BLOCK_SIZE - 1);
            } else {
                ProducerIdBlock currProducerIdBlock;
                try {
                    currProducerIdBlock = ProducerIdManager.parseProducerIdBlockData(dataAndVersion.data);
                } catch (Exception e) {
                    log.error("Failed to parse producerIdBlock data.", e);
                    setDataFuture.completeExceptionally(e);
                    return;
                }
                if (currProducerIdBlock.blockEndId > Long.MAX_VALUE - PID_BLOCK_SIZE) {
                    log.error("Exhausted all producerIds as the next block's end producerId "
                            + "is will has exceeded long type limit (current block end producerId is {})",
                            currProducerIdBlock.blockEndId);
                    setDataFuture.completeExceptionally(new Exception("Have exhausted all producerIds."));
                    return;
                }
                currentProducerIdBlock = new ProducerIdBlock(brokerId,
                        currProducerIdBlock.blockEndId + 1,
                        currProducerIdBlock.blockEndId + PID_BLOCK_SIZE);
            }

            try {
                byte[] newProducerIdBlockData = ProducerIdManager.generateProducerIdBlockJson(currentProducerIdBlock);
                conditionalSetData(newProducerIdBlockData, dataAndVersion == null ? -1 : dataAndVersion.zkVersion)
                        .whenComplete(((setDataResult, setDataThrowable) -> {
                            if (setDataThrowable != null) {
                                log.error("Failed to set new producerId block.", setDataThrowable);
                                setDataFuture.completeExceptionally(setDataThrowable);
                                return;
                            }
                            log.info("Acquire new producerId block {} by writing to Zk with path version {}",
                                    currentProducerIdBlock, setDataResult.zkVersion);
                            setDataFuture.complete(null);
                        }));
            } catch (JsonProcessingException e) {
                log.error("Failed to generate producerIdBlock json bytes data. pidBlock: {}",
                        currentProducerIdBlock, e);
            }
        });
        return setDataFuture;
    }

    public CompletableFuture<DataAndVersion> getDataAndVersion() {
        CompletableFuture<DataAndVersion> currentPidData = new CompletableFuture<>();
        try {
            zkClient.getData(KOP_PID_BLOCK_ZNODE, null, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (rc == KeeperException.Code.OK.intValue() && data != null) {
                        currentPidData.complete(new DataAndVersion(data, stat.getVersion()));
                    } else {
                        currentPidData.complete(null);
                    }
                }
            }, null);
        } catch (Exception e) {
            log.error("Failed to get producerId block data.", e);
            currentPidData.completeExceptionally(e);
        }
        return currentPidData;
    }

    private CompletableFuture<SetDataResult> conditionalSetData(byte[] data, int version) {
        CompletableFuture<SetDataResult> updateFuture = new CompletableFuture<>();
        zkClient.setData(KOP_PID_BLOCK_ZNODE, data, version, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    updateFuture.complete(new SetDataResult(stat.getVersion()));
                } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                    checkProducerIdBlockZkData(zkClient, data)
                            .whenComplete((setDataResult, t)-> {
                                if (t != null) {
                                    updateFuture.completeExceptionally(t);
                                } else {
                                    updateFuture.complete(setDataResult);
                                }
                            });
                } else if (rc == KeeperException.Code.NONODE.intValue()){
                    log.error("Update of path {} with data {} and expected version {} failed due to {}",
                            KOP_PID_BLOCK_ZNODE, getProducerIdBlockStr(data), stat.getVersion(),
                            "NoNode for path " + KOP_PID_BLOCK_ZNODE);
                    updateFuture.completeExceptionally(new Exception("NoNode for path " + KOP_PID_BLOCK_ZNODE));
                } else {
                    log.error("Update of path {} with data {} and expected version {} keeperException code {}",
                            KOP_PID_BLOCK_ZNODE, getProducerIdBlockStr(data), stat.getVersion(), rc);
                    updateFuture.completeExceptionally(new Exception("KeeperException code " + rc));
                }
            }
        }, null);
        return updateFuture;
    }

    private CompletableFuture<SetDataResult> checkProducerIdBlockZkData(ZooKeeper zkClient, byte[] expectedData) {
        CompletableFuture<SetDataResult> resultFuture = new CompletableFuture<>();
        try {
            ProducerIdBlock expectedPidBlock = ProducerIdManager.parseProducerIdBlockData(expectedData);
            zkClient.getData(KOP_PID_BLOCK_ZNODE, null, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object o, byte[] bytes, Stat stat) {
                    try {
                        ProducerIdBlock currentPidBlock = ProducerIdManager.parseProducerIdBlockData(bytes);
                        if (expectedPidBlock.equals(currentPidBlock)) {
                            resultFuture.complete(new SetDataResult(stat.getVersion()));
                        } else {
                            resultFuture.completeExceptionally(new Exception(""));
                        }
                    } catch (IOException e) {
                        log.error("Failed to parse producerIdBlock data {}.", getProducerIdBlockStr(bytes), e);
                        resultFuture.completeExceptionally(e);
                    }
                }
            }, null);
        } catch (Exception e) {
            log.error("Error while checking for producerId block Zk data on path {}: expected data {}",
                    KOP_PID_BLOCK_ZNODE, getProducerIdBlockStr(expectedData), e);
            resultFuture.completeExceptionally(e);
        }
        return resultFuture;
    }

    @AllArgsConstructor
    private static class SetDataResult {
        private int zkVersion;
    }

    @AllArgsConstructor
    private static class DataAndVersion {
        private byte[] data;
        private int zkVersion;
    }

    private void makeSurePathExists() {
        ZooKeeperUtils.tryCreatePath(zkClient, KOP_PID_BLOCK_ZNODE, null);
    }

    public void initialize() {
        makeSurePathExists();
        getNewProducerIdBlock();
        nextProducerId = currentProducerIdBlock.blockStartId;
    }

    public synchronized CompletableFuture<Long> generateProducerId() {
        CompletableFuture<Long> nextProducerIdFuture = new CompletableFuture<>();
        // grab a new block of producerIds if this block has been exhausted
        if (nextProducerId > currentProducerIdBlock.blockEndId) {
            getNewProducerIdBlock().whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    nextProducerIdFuture.completeExceptionally(throwable);
                    return;
                }
                nextProducerId = currentProducerIdBlock.blockStartId + 1;
                nextProducerIdFuture.complete(nextProducerId - 1);
            });
        } else {
            nextProducerId += 1;
            nextProducerIdFuture.complete(nextProducerId - 1);
        }

        return nextProducerIdFuture;
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

    public void shutdown() {
        log.info("Shutdown complete: last producerId assigned {}", nextProducerId);
    }

}
