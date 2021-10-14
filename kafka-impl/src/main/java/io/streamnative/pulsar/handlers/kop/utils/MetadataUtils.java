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

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.internals.Topic;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;

/**
 * Utils for KoP Metadata.
 */
@Slf4j
public class MetadataUtils {
    // trigger compaction when the offset backlog reaches 100MB
    public static final int MAX_COMPACTION_THRESHOLD = 100 * 1024 * 1024;

    public static String constructOffsetsTopicBaseName(String tenant, KafkaServiceConfiguration conf) {
        return tenant + "/" + conf.getKafkaMetadataNamespace()
            + "/" + Topic.GROUP_METADATA_TOPIC_NAME;
    }

    public static String constructTxnLogTopicBaseName(String tenant, KafkaServiceConfiguration conf) {
        return tenant + "/" + conf.getKafkaMetadataNamespace()
                + "/" + Topic.TRANSACTION_STATE_TOPIC_NAME;
    }

    public static String constructSchemaRegistryTopicName(String tenant, KafkaServiceConfiguration conf) {
        return tenant + "/" + conf.getKafkaMetadataNamespace()
                + "/" + conf.getKopSchemaRegistryTopicName();
    }

    public static void createOffsetMetadataIfMissing(String tenant, PulsarAdmin pulsarAdmin,
                                                     ClusterData clusterData,
                                                     KafkaServiceConfiguration conf)
            throws PulsarAdminException {
        KopTopic kopTopic = new KopTopic(constructOffsetsTopicBaseName(tenant, conf));
        createKafkaMetadataIfMissing(tenant, pulsarAdmin, clusterData, conf, kopTopic,
                conf.getOffsetsTopicNumPartitions());
    }

    public static void createTxnMetadataIfMissing(String tenant,
                                                  PulsarAdmin pulsarAdmin,
                                                  ClusterData clusterData,
                                                  KafkaServiceConfiguration conf)
            throws PulsarAdminException {
        KopTopic kopTopic = new KopTopic(constructTxnLogTopicBaseName(tenant, conf));
        createKafkaMetadataIfMissing(tenant, pulsarAdmin, clusterData, conf, kopTopic,
                conf.getTxnLogTopicNumPartitions());
    }

    public static void createSchemaRegistryMetadataIfMissing(String tenant,
                                                  PulsarAdmin pulsarAdmin,
                                                  ClusterData clusterData,
                                                  KafkaServiceConfiguration conf)
            throws PulsarAdminException {
        KopTopic kopTopic = new KopTopic(constructSchemaRegistryTopicName(tenant, conf));
        createKafkaMetadataIfMissing(tenant, pulsarAdmin, clusterData, conf, kopTopic, 1);
    }

    /**
     * This method creates the Kafka metadata tenant and namespace if they are not currently present.
     * <ul>
     * <li>If the cluster does not exist this method will throw a PulsarServerException.NotFoundException</li>
     * <li>If the tenant does not exist it will be created</li>
     * <li>If the tenant exists but the allowedClusters list does not include the cluster this method will
     * add the cluster to the allowedClusters list</li>
     * <li>If the namespace does not exist it will be created</li>
     * <li>If the namespace exists but the replicationClusters list does not include the cluster this method
     * will add the cluster to the replicationClusters list</li>
     * <li>If the offset topic does not exist it will be created</li>
     * <li>If the offset topic exists but some partitions are missing, the missing partitions will be created</li>
     * </ul>
     */
    private static void createKafkaMetadataIfMissing(String tenant,
                                                     PulsarAdmin pulsarAdmin,
                                                     ClusterData clusterData,
                                                     KafkaServiceConfiguration conf,
                                                     KopTopic kopTopic,
                                                     int partitionNum)
        throws PulsarAdminException {
        String cluster = conf.getClusterName();
        String kafkaMetadataNamespace = tenant + "/" + conf.getKafkaMetadataNamespace();

        boolean clusterExists = false;
        boolean tenantExists = false;
        boolean namespaceExists = false;
        boolean offsetsTopicExists = false;

        try {
            Clusters clusters = pulsarAdmin.clusters();
            if (!clusters.getClusters().contains(cluster)) {
                try {
                    pulsarAdmin.clusters().createCluster(cluster, clusterData);
                } catch (PulsarAdminException e) {
                    if (e instanceof ConflictException) {
                        log.info("Attempted to create cluster {} however it was created concurrently.", cluster);
                    } else {
                        // Re-throw all other exceptions
                        throw e;
                    }
                }
            } else {
                ClusterData configuredClusterData = clusters.getCluster(cluster);
                log.info("Cluster {} found: {}", cluster, configuredClusterData);
            }
            clusterExists = true;

            // Check if the metadata tenant exists and create it if not
            Tenants tenants = pulsarAdmin.tenants();
            createTenantIfMissing(tenant, conf, cluster, tenants);
            tenantExists = true;

            // Check if the metadata namespace exists and create it if not
            Namespaces namespaces = pulsarAdmin.namespaces();
            createNamespaceIfMissing(tenant, conf, cluster, kafkaMetadataNamespace, namespaces);

            namespaceExists = true;

            // Check if the offsets topic exists and create it if not
            createTopicIfNotExist(pulsarAdmin, kopTopic.getFullName(), partitionNum);
            offsetsTopicExists = true;
        } catch (PulsarAdminException e) {
            if (e instanceof ConflictException) {
                log.info("Resources concurrent creating and cause e: ", e);
                return;
            }

            log.error("Failed to successfully initialize Kafka Metadata {}",
                kafkaMetadataNamespace, e);
            throw e;
        } finally {
            log.info("Current state of kafka metadata, cluster: {} exists: {}, tenant: {} exists: {},"
                            + " namespace: {} exists: {}, topic: {} exists: {}",
                    cluster, clusterExists, tenant, tenantExists, kafkaMetadataNamespace, namespaceExists,
                    kopTopic.getOriginalName(), offsetsTopicExists);
        }
    }

    private static void createTenantIfMissing(String tenant, KafkaServiceConfiguration conf,
                                              String cluster, Tenants tenants) throws PulsarAdminException {
        if (!tenants.getTenants().contains(tenant)) {
            log.info("Tenant: {} does not exist, creating it ...", tenant);
            tenants.createTenant(tenant,
                    TenantInfo.builder()
                            .adminRoles(conf.getSuperUserRoles())
                            .allowedClusters(Collections.singleton(cluster))
                            .build());
        } else {
            TenantInfo kafkaMetadataTenantInfo = tenants.getTenantInfo(tenant);
            Set<String> allowedClusters = kafkaMetadataTenantInfo.getAllowedClusters();
            if (!allowedClusters.contains(cluster)) {
                log.info("Tenant: {} exists but cluster: {} is not in the allowedClusters list, updating it ...",
                        tenant, cluster);
                allowedClusters.add(cluster);
                tenants.updateTenant(tenant, kafkaMetadataTenantInfo);
            }
        }
    }

    private static void createNamespaceIfMissing(String tenant, KafkaServiceConfiguration conf, String cluster,
                                                 String kafkaMetadataNamespace, Namespaces namespaces)
                                                 throws PulsarAdminException {
        if (!namespaces.getNamespaces(tenant).contains(kafkaMetadataNamespace)) {
            log.info("Namespaces: {} does not exist in tenant: {}, creating it ...",
                    kafkaMetadataNamespace, tenant);
            Set<String> replicationClusters = Sets.newHashSet(cluster);
            namespaces.createNamespace(kafkaMetadataNamespace, replicationClusters);
            namespaces.setNamespaceReplicationClusters(kafkaMetadataNamespace, replicationClusters);
        } else {
            List<String> replicationClusters = namespaces.getNamespaceReplicationClusters(kafkaMetadataNamespace);
            if (!replicationClusters.contains(cluster)) {
                log.info("Namespace: {} exists but cluster: {} is not in the replicationClusters list,"
                        + "updating it ...", kafkaMetadataNamespace, cluster);
                Set<String> newReplicationClusters = Sets.newHashSet(replicationClusters);
                newReplicationClusters.add(cluster);
                namespaces.setNamespaceReplicationClusters(kafkaMetadataNamespace, newReplicationClusters);
            }
        }
        // set namespace config if namespace existed
        int retentionMinutes = (int) conf.getOffsetsRetentionMinutes();
        RetentionPolicies retentionPolicies = namespaces.getRetention(kafkaMetadataNamespace);
        if (retentionPolicies == null || retentionPolicies.getRetentionTimeInMinutes() != retentionMinutes) {
            namespaces.setRetention(kafkaMetadataNamespace,
                    new RetentionPolicies((int) conf.getOffsetsRetentionMinutes(), -1));
        }

        Long compactionThreshold = namespaces.getCompactionThreshold(kafkaMetadataNamespace);
        if (compactionThreshold != null && compactionThreshold != MAX_COMPACTION_THRESHOLD) {
            namespaces.setCompactionThreshold(kafkaMetadataNamespace, MAX_COMPACTION_THRESHOLD);
        }

        int targetMessageTTL = conf.getOffsetsMessageTTL();
        Integer messageTTL = namespaces.getNamespaceMessageTTL(kafkaMetadataNamespace);
        if (messageTTL == null || messageTTL != targetMessageTTL) {
            namespaces.setNamespaceMessageTTL(kafkaMetadataNamespace, targetMessageTTL);
        }
    }

    /**
     * This method creates the Kafka tenant and namespace if they are not currently present.
     * <ul>
     * <li>If the cluster does not exist this method will throw a PulsarServerException.NotFoundException</li>
     * <li>If the tenant does not exist it will be created</li>
     * <li>If the tenant exists but the allowedClusters list does not include the cluster this method will
     * add the cluster to the allowedClusters list</li>
     * <li>If the namespace does not exist it will be created</li>
     * <li>If the namespace exists but the replicationClusters list does not include the cluster this method
     * will add the cluster to the replicationClusters list</li>
     * <li>If the offset topic does not exist it will be created</li>
     * <li>If the offset topic exists but some partitions are missing, the missing partitions will be created</li>
     * </ul>
     */
    public static void createKafkaNamespaceIfMissing(PulsarAdmin pulsarAdmin,
                                                     ClusterData clusterData,
                                                     KafkaServiceConfiguration conf)
            throws PulsarAdminException {
        String cluster = conf.getClusterName();
        String tenant = conf.getKafkaTenant();
        String kafkaNamespace = tenant + "/" + conf.getKafkaNamespace();

        boolean clusterExists = false;
        boolean tenantExists = false;
        boolean namespaceExists = false;

        try {
            Clusters clusters = pulsarAdmin.clusters();
            if (!clusters.getClusters().contains(cluster)) {
                try {
                    pulsarAdmin.clusters().createCluster(cluster, clusterData);
                } catch (PulsarAdminException e) {
                    if (e instanceof ConflictException) {
                        log.info("Attempted to create cluster {} however it was created concurrently.", cluster);
                    } else {
                        // Re-throw all other exceptions
                        throw e;
                    }
                }
            } else {
                ClusterData configuredClusterData = clusters.getCluster(cluster);
                log.info("Cluster {} found: {}", cluster, configuredClusterData);
            }
            clusterExists = true;

            // Check if the kafka tenant exists and create it if not
            Tenants tenants = pulsarAdmin.tenants();
            createTenantIfMissing(tenant, conf, cluster, tenants);
            tenantExists = true;

            Namespaces namespaces = pulsarAdmin.namespaces();
            // Check if the kafka namespace exists and create it if not
            createNamespaceIfMissing(tenant, conf, cluster, kafkaNamespace, namespaces);
            namespaceExists = true;

        } catch (PulsarAdminException e) {
            if (e instanceof ConflictException) {
                log.info("Resources concurrent creating and cause e: ", e);
                return;
            }

            log.error("Failed to successfully initialize Kafka Metadata {}",
                    kafkaNamespace, e);
            throw e;
        } finally {
            log.info("Current state of kafka metadata, cluster: {} exists: {}, tenant: {} exists: {},"
                            + " namespace: {} exists: {}",
                    cluster, clusterExists, tenant, tenantExists, kafkaNamespace, namespaceExists);
        }
    }

    private static void createTopicIfNotExist(final PulsarAdmin admin,
                                              final String topic,
                                              final int numPartitions) throws PulsarAdminException {
        if (numPartitions > 1) {
            try {
                admin.topics().createPartitionedTopic(topic, numPartitions);
            } catch (PulsarAdminException.ConflictException e) {
                log.info("Resources concurrent creating for topic : {}, caused by : {}", topic, e.getMessage());
            }
            try {
                // Ensure all partitions are created
                admin.topics().createMissedPartitions(topic);
            } catch (PulsarAdminException ignored) {
            }
        } else {
            try {
                admin.topics().createNonPartitionedTopic(topic);
            } catch (PulsarAdminException.ConflictException e) {
                log.info("Resources concurrent creating for topic : {}, caused by : {}", topic, e.getMessage());
            }
        }
    }
}
