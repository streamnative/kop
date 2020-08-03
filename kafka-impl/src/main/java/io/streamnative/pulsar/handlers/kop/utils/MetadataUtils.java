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

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.Sets;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.internals.Topic;
import org.apache.pulsar.broker.PulsarServerException;
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

    public static String constructOffsetsTopicBaseName(KafkaServiceConfiguration conf) {
        return conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace()
            + "/" + Topic.GROUP_METADATA_TOPIC_NAME;
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
    public static void createKafkaMetadataIfMissing(PulsarAdmin pulsarAdmin, KafkaServiceConfiguration conf)
        throws PulsarServerException, PulsarAdminException {
        String cluster = conf.getClusterName();
        String kafkaMetadataTenant = conf.getKafkaMetadataTenant();
        String kafkaMetadataNamespace = kafkaMetadataTenant + "/" + conf.getKafkaMetadataNamespace();

        String offsetsTopic = constructOffsetsTopicBaseName(conf);

        boolean clusterExists, tenantExists, namespaceExists, offsetsTopicExists;
        clusterExists = tenantExists = namespaceExists = offsetsTopicExists = false;

        try {
            Clusters clusters = pulsarAdmin.clusters();
            if (!clusters.getClusters().contains(cluster)) {
                throw new PulsarServerException.NotFoundException("Configured cluster does not exist");
            } else {
                ClusterData configuredClusterData = clusters.getCluster(cluster);
                log.info("Cluster {} found: {}", cluster, configuredClusterData);
                clusterExists = true;
            }

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
                Set<String> replicationClusters = Sets.newHashSet(conf.getClusterName());
                namespaces.createNamespace(kafkaMetadataNamespace, replicationClusters);
                namespaces.setNamespaceReplicationClusters(kafkaMetadataNamespace, replicationClusters);
                namespaces.setRetention(kafkaMetadataNamespace,
                    new RetentionPolicies(-1, -1));
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
            namespaceExists = true;

            // Check if the offsets topic exists and create it if not
            Topics topics = pulsarAdmin.topics();
            PartitionedTopicMetadata offsetsTopicMetadata = topics.getPartitionedTopicMetadata(offsetsTopic);

            Set<String> offsetPartitionSet = new HashSet<String>(conf.getOffsetsTopicNumPartitions());
            for (int i = 0; i < conf.getOffsetsTopicNumPartitions(); i++) {
                offsetPartitionSet.add(offsetsTopic + PARTITIONED_TOPIC_SUFFIX + i);
            }

            if (offsetsTopicMetadata.partitions <= 0) {
                log.info("Kafka group metadata topic {} doesn't exist. Creating it ...", offsetsTopic);

                topics.createPartitionedTopic(
                        offsetsTopic,
                        conf.getOffsetsTopicNumPartitions()
                );

                for (String partition : offsetPartitionSet) {
                    topics.createNonPartitionedTopic(partition);
                }

                log.info("Successfully created group metadata topic {} with {} partitions.",
                        offsetsTopic, conf.getOffsetsTopicNumPartitions());
            } else {
                // Check to see if the partitions all exist
                offsetPartitionSet.removeAll(
                topics.getList(kafkaMetadataNamespace).stream()
                .filter((topic) -> {
                    return topic.startsWith(offsetsTopic + PARTITIONED_TOPIC_SUFFIX);
                }).collect(Collectors.toList()));

                if (!offsetPartitionSet.isEmpty()) {
                    log.info("Identified missing offset topic partitions: {}", offsetPartitionSet);
                    for (String offsetPartition : offsetPartitionSet) {
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
                + " namespace: {} exists: {}, topic: {} exists: {}", cluster, clusterExists, kafkaMetadataTenant,
                tenantExists, kafkaMetadataNamespace, namespaceExists, offsetsTopic, offsetsTopicExists);
        }
    }
}
