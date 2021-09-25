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
package io.streamnative.pulsar.handlers.kop.utils;

import java.nio.charset.StandardCharsets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * Utils for ZooKeeper.
 */
@Slf4j
public class ZooKeeperUtils {

    public static void tryCreatePath(ZooKeeper zooKeeper, String path, byte[] data) {
        tryCreatePath(zooKeeper, path, data, true);
    }

    public static void tryCreatePath(ZooKeeper zooKeeper, String path, byte[] data, boolean updateDataIfExists) {
        try {
            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            if (log.isDebugEnabled()) {
                log.debug("Created ZK path: {} data: {}",
                        path, data == null ? "null" : new String(data, StandardCharsets.UTF_8));
            }
        } catch (KeeperException e) {
            if (!e.code().equals(KeeperException.Code.NODEEXISTS)) {
                log.error("Failed to create ZooKeeper node {}: {}", path, e.getMessage());
            } else {
                if (updateDataIfExists) {
                    // update the group id
                    try {
                        zooKeeper.setData(path, data, -1);
                        if (log.isDebugEnabled()) {
                            log.debug("Updated ZK path: {} data: {}",
                                    path, data == null ? "null" : new String(data, StandardCharsets.UTF_8));
                        }
                    } catch (KeeperException | InterruptedException setDataException) {
                        log.error("Failed to set path '{}''s data to {}", path, data, setDataException);
                    }
                }
            }
        } catch (InterruptedException e) {
            log.error("Failed to create ZooKeeper node {}: {}", path, e.getMessage());
        }
    }

    public static @NonNull String getData(ZooKeeper zooKeeper, String zkPath, String subPath) {
        try {
            String addSubPath = zkPath + subPath;
            Stat zkStat = zooKeeper.exists(addSubPath, true);
            if (zkStat != null) {
                return new String(zooKeeper.getData(addSubPath, false, zkStat), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.error("get zookeeper path data error", e);
        }
        return "";
    }

    public static String groupIdPathFormat(String clientHost, String clientId) {
        String path = clientHost.split(":")[0] + "-" + clientId;
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }
}