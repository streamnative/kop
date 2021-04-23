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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.internals.Topic;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
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

    public static String constructOffsetsTopicBaseName(KafkaServiceConfiguration conf) {
        return conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace()
            + "/" + Topic.GROUP_METADATA_TOPIC_NAME;
    }

    public static String constructTxnLogTopicBaseName(KafkaServiceConfiguration conf) {
        return conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace()
                + "/" + Topic.TRANSACTION_STATE_TOPIC_NAME;
    }

    public static void createOffsetMetadataIfMissing(PulsarAdmin pulsarAdmin,
                                                     ClusterData clusterData,
                                                     KafkaServiceConfiguration conf)
            throws PulsarAdminException {
        KopTopic kopTopic = new KopTopic(constructOffsetsTopicBaseName(conf));
        createKafkaMetadataIfMissing(pulsarAdmin, clusterData, conf, kopTopic, conf.getOffsetsTopicNumPartitions());
    }

    public static void createTxnMetadataIfMissing(PulsarAdmin pulsarAdmin,
                                                  ClusterData clusterData,
                                                  KafkaServiceConfiguration conf)
            throws PulsarAdminException {
        KopTopic kopTopic = new KopTopic(constructTxnLogTopicBaseName(conf));
        createKafkaMetadataIfMissing(pulsarAdmin, clusterData, conf, kopTopic, conf.getTxnLogTopicNumPartitions());
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
    private static void createKafkaMetadataIfMissing(PulsarAdmin pulsarAdmin,
                                                     ClusterData clusterData,
                                                     KafkaServiceConfiguration conf,
                                                     KopTopic kopTopic,
                                                     int partitionNum)
        throws PulsarAdminException {
        String cluster = conf.getClusterName();
        String kafkaMetadataTenant = conf.getKafkaMetadataTenant();
        String kafkaMetadataNamespace = kafkaMetadataTenant + "/" + conf.getKafkaMetadataNamespace();

        boolean clusterExists, tenantExists, namespaceExists, offsetsTopicExists;
        clusterExists = tenantExists = namespaceExists = offsetsTopicExists = false;

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
            if (!tenants.getTenants().contains(kafkaMetadataTenant)) {
                log.info("Tenant: {} does not exist, creating it ...", kafkaMetadataTenant);
                tenants.createTenant(kafkaMetadataTenant,
                    new TenantInfo(Sets.newHashSet(conf.getSuperUserRoles()), Sets.newHashSet(cluster)));
            } else {
                TenantInfo kafkaMetadataTenantInfo = tenants.getTenantInfo(kafkaMetadataTenant);
                Set<String> allowedClusters = kafkaMetadataTenantInfo.getAllowedClusters();
                if (!allowedClusters.contains(cluster)) {
                    log.info("Tenant: {} exists but cluster: {} is not in the allowedClusters list, updating it ...",
                        kafkaMetadataTenant, cluster);
                    allowedClusters.add(cluster);
                    tenants.updateTenant(kafkaMetadataTenant, kafkaMetadataTenantInfo);
                }
            }
            tenantExists = true;

            // Check if the metadata namespace exists and create it if not
            Namespaces namespaces = pulsarAdmin.namespaces();
            if (!namespaces.getNamespaces(kafkaMetadataTenant).contains(kafkaMetadataNamespace)) {
                log.info("Namespaces: {} does not exist in tenant: {}, creating it ...",
                    kafkaMetadataNamespace, kafkaMetadataTenant);
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

            namespaceExists = true;

            // Check if the offsets topic exists and create it if not
            Topics topics = pulsarAdmin.topics();
            PartitionedTopicMetadata topicMetadata =
                    topics.getPartitionedTopicMetadata(kopTopic.getFullName());

            Set<String> partitionSet = new HashSet<>(partitionNum);
            for (int i = 0; i < partitionNum; i++) {
                partitionSet.add(kopTopic.getPartitionName(i));
            }

            if (topicMetadata.partitions <= 0) {
                log.info("Kafka group metadata topic {} doesn't exist. Creating it ...", kopTopic.getFullName());

                topics.createPartitionedTopic(
                        kopTopic.getFullName(),
                        partitionNum
                );

                log.info("Successfully created kop metadata topic {} with {} partitions.",
                        kopTopic.getFullName(), partitionNum);
            } else {
                // Check to see if the partitions all exist
                partitionSet.removeAll(
                        topics.getList(kafkaMetadataNamespace).stream()
                                .filter((topic) -> topic.startsWith(kopTopic.getFullName()))
                                .collect(Collectors.toList())
                );

                if (!partitionSet.isEmpty()) {
                    log.info("Identified missing kop metadata topic {} partitions: {}", kopTopic, partitionSet);
                    for (String offsetPartition : partitionSet) {
                        topics.createNonPartitionedTopic(offsetPartition);
                    }
                }
            }
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
                    cluster, clusterExists, kafkaMetadataTenant, tenantExists, kafkaMetadataNamespace, namespaceExists,
                    kopTopic.getOriginalName(), offsetsTopicExists);
        }
    }
}
