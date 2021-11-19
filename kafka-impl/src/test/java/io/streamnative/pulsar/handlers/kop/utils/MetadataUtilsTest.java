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

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.Test;

/**
* Validate MetadataUtils.
*/
public class MetadataUtilsTest {

    @Test(timeOut = 30000)
    public void testCreateKafkaMetadataIfMissing() throws Exception {
        String namespacePrefix = "public/default";
        KafkaServiceConfiguration conf = new KafkaServiceConfiguration();
        ClusterData clusterData = ClusterData.builder().build();
        conf.setClusterName("test");
        conf.setKafkaMetadataTenant("public");
        conf.setKafkaMetadataNamespace("default");
        conf.setSuperUserRoles(Sets.newHashSet("admin"));
        conf.setOffsetsTopicNumPartitions(8);

        final KopTopic offsetsTopic = new KopTopic(MetadataUtils
                .constructOffsetsTopicBaseName(conf.getKafkaMetadataTenant(), conf), namespacePrefix);
        final KopTopic txnTopic = new KopTopic(MetadataUtils
                .constructTxnLogTopicBaseName(conf.getKafkaMetadataTenant(), conf), namespacePrefix);

        List<String> emptyList = Lists.newArrayList();

        List<String> existingClusters = Lists.newArrayList("test");
        Clusters mockClusters = mock(Clusters.class);
        doReturn(existingClusters).when(mockClusters).getClusters();

        Tenants mockTenants = mock(Tenants.class);
        doReturn(emptyList).when(mockTenants).getTenants();

        Namespaces mockNamespaces = mock(Namespaces.class);
        doReturn(emptyList).when(mockNamespaces).getNamespaces("public");

        PartitionedTopicMetadata offsetTopicMetadata = new PartitionedTopicMetadata();
        Topics mockTopics = mock(Topics.class);
        doReturn(offsetTopicMetadata).when(mockTopics).getPartitionedTopicMetadata(eq(offsetsTopic.getFullName()));
        doReturn(offsetTopicMetadata).when(mockTopics).getPartitionedTopicMetadata(eq(txnTopic.getFullName()));

        PulsarAdmin mockPulsarAdmin = mock(PulsarAdmin.class);

        doReturn(mockClusters).when(mockPulsarAdmin).clusters();
        doReturn(mockTenants).when(mockPulsarAdmin).tenants();
        doReturn(mockNamespaces).when(mockPulsarAdmin).namespaces();
        doReturn(mockTopics).when(mockPulsarAdmin).topics();

        TenantInfo partialTenant = TenantInfo.builder().build();
        doReturn(partialTenant).when(mockTenants).getTenantInfo(eq(conf.getKafkaMetadataTenant()));

        MetadataUtils.createOffsetMetadataIfMissing(conf.getKafkaMetadataTenant(), mockPulsarAdmin, clusterData, conf);

        // After call the createOffsetMetadataIfMissing, these methods should return expected data.
        doReturn(Lists.newArrayList(conf.getKafkaMetadataTenant())).when(mockTenants).getTenants();
        String namespace = conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace();
        doReturn(Lists.newArrayList(namespace)).when(mockNamespaces).getNamespaces(conf.getKafkaMetadataTenant());
        doReturn(Lists.newArrayList(conf.getClusterName())).when(mockNamespaces)
                .getNamespaceReplicationClusters(eq(namespace));

        MetadataUtils.createTxnMetadataIfMissing(conf.getKafkaMetadataTenant(), mockPulsarAdmin, clusterData, conf);

        verify(mockTenants, times(1)).createTenant(eq(conf.getKafkaMetadataTenant()), any(TenantInfo.class));
        verify(mockNamespaces, times(1)).createNamespace(eq(conf.getKafkaMetadataTenant() + "/"
            + conf.getKafkaMetadataNamespace()), any(Set.class));
        verify(mockNamespaces, times(1)).setNamespaceReplicationClusters(eq(conf.getKafkaMetadataTenant()
            + "/" + conf.getKafkaMetadataNamespace()), any(Set.class));
        verify(mockNamespaces, times(2)).setRetention(eq(conf.getKafkaMetadataTenant() + "/"
            + conf.getKafkaMetadataNamespace()), any(RetentionPolicies.class));
        verify(mockNamespaces, times(2)).setNamespaceMessageTTL(eq(conf.getKafkaMetadataTenant() + "/"
            + conf.getKafkaMetadataNamespace()), any(Integer.class));
        verify(mockTopics, times(1)).createPartitionedTopic(
                eq(offsetsTopic.getFullName()), eq(conf.getOffsetsTopicNumPartitions()));
        verify(mockTopics, times(1)).createPartitionedTopic(
                eq(txnTopic.getFullName()), eq(conf.getTxnLogTopicNumPartitions()));

        // Test that cluster is added to existing Tenant if missing
        // Test that the cluster is added to the namespace replication cluster list if it is missing
        // Test that missing offset topic partitions are created

        reset(mockTenants);
        reset(mockNamespaces);
        reset(mockTopics);

        doReturn(Lists.newArrayList("public")).when(mockTenants).getTenants();

        partialTenant = TenantInfo.builder()
                .adminRoles(conf.getSuperUserRoles())
                .allowedClusters(Sets.newHashSet("other-cluster"))
                .build();
        doReturn(partialTenant).when(mockTenants).getTenantInfo(eq(conf.getKafkaMetadataTenant()));

        doReturn(Lists.newArrayList("test")).when(mockNamespaces).getNamespaces("public");
        doReturn(emptyList).when(mockNamespaces).getNamespaceReplicationClusters(eq(conf.getKafkaMetadataTenant()));

        List<String> incompletePartitionList = new ArrayList<String>(conf.getOffsetsTopicNumPartitions());
        for (int i = 0; i < conf.getOffsetsTopicNumPartitions() - 2; i++) {
            incompletePartitionList.add(offsetsTopic.getPartitionName(i));
        }
        for (int i = 0; i < conf.getTxnLogTopicNumPartitions() - 2; i++) {
            incompletePartitionList.add(txnTopic.getPartitionName(i));
        }

        doReturn(new PartitionedTopicMetadata(8)).when(mockTopics)
                .getPartitionedTopicMetadata(eq(offsetsTopic.getFullName()));
        doReturn(new PartitionedTopicMetadata(8)).when(mockTopics)
                .getPartitionedTopicMetadata(eq(txnTopic.getFullName()));
        doReturn(incompletePartitionList).when(mockTopics).getList(eq(conf.getKafkaMetadataTenant()
            + "/" + conf.getKafkaMetadataNamespace()));

        MetadataUtils.createOffsetMetadataIfMissing(conf.getKafkaMetadataTenant(), mockPulsarAdmin, clusterData, conf);
        MetadataUtils.createTxnMetadataIfMissing(conf.getKafkaMetadataTenant(), mockPulsarAdmin, clusterData, conf);

        verify(mockTenants, times(1)).updateTenant(eq(conf.getKafkaMetadataTenant()), any(TenantInfo.class));
        verify(mockNamespaces, times(2)).setNamespaceReplicationClusters(eq(conf.getKafkaMetadataTenant()
            + "/" + conf.getKafkaMetadataNamespace()), any(Set.class));
        verify(mockTopics, times(1)).createMissedPartitions(contains(offsetsTopic.getOriginalName()));
        verify(mockTopics, times(1)).createMissedPartitions(contains(txnTopic.getOriginalName()));
    }
}
