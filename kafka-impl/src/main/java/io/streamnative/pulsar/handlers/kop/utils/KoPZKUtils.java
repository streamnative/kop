package io.streamnative.pulsar.handlers.kop.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Utils for ZooKeeper.
 */
@Slf4j
public class KoPZKUtils {
    public static void createPath(ZooKeeper zooKeeper, String zkPath, String subPath, byte[] data) {
        try {
            if (zooKeeper.exists(zkPath, false) == null) {
                zooKeeper.create(zkPath,
                        data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String addSubPath = zkPath + subPath;
            if (zooKeeper.exists(addSubPath, false) == null) {
                zooKeeper.create(addSubPath,
                        data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zooKeeper.setData(addSubPath, data, -1);
            }
            log.debug("create zk path, addSubPath:{} data:{}.",
                    addSubPath, new String(data));
        } catch (Exception e) {
            log.error("create zookeeper path error", e);
        }
    }

    public static String getData(ZooKeeper zooKeeper, String zkPath, String subPath) {
        String data = "";
        try {
            String addSubPath = zkPath + subPath;
            Stat zkStat = zooKeeper.exists(addSubPath, true);
            if (zkStat != null) {
                data = new String(zooKeeper.getData(addSubPath, false, zkStat));
            }
        } catch (Exception e) {
            log.error("get zookeeper path data error", e);
        }
        return data;
    }
}
