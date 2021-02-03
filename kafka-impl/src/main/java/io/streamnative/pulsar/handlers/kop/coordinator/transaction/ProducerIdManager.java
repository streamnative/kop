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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * This class used to manage producer id.
 */
public class ProducerIdManager {

    private final AtomicLong producerId = new AtomicLong(0);
    private final String pidBlockZNode = "/kop_latest_producer_id_block";
    private final ZooKeeper zkClient;

    public ProducerIdManager(ZooKeeper zkClient) {
        this.zkClient = zkClient;
    }

    public long generateProducerId() {
        // TODO generate unique producer id
        return producerId.incrementAndGet();
    }

    public CompletableFuture<DataAndVersion> getPidDataAndVersion() {
        CompletableFuture<DataAndVersion> future = new CompletableFuture<>();
        zkClient.getData(pidBlockZNode, null, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc != KeeperException.Code.OK.intValue()) {
                    future.complete(new DataAndVersion(data, stat.getVersion()));
                } else {
                    future.complete(null);
                }
            }
        }, null);
        return future;
    }

    @AllArgsConstructor
    private static class DataAndVersion {
        private byte[] data;
        private int zkVersion;
    }

}
