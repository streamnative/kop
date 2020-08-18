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
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.Test;

/**
* Validate MetadataUtils.
*/
public class MetadataUtilsTest {

    @Test(timeOut = 20000)
    public void testCreateKafkaMetadataIfMissing() throws Exception {
        KafkaServiceConfiguration conf = new KafkaServiceConfiguration();
        conf.setClusterName("test");
        conf.setKafkaMetadataTenant("public");
        conf.setKafkaMetadataNamespace("default");
        conf.setSuperUserRoles(Sets.newHashSet("admin"));
        conf.setOffsetsTopicNumPartitions(8);

        String offsetsTopic = MetadataUtils.constructOffsetsTopicBaseName(conf);

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
        doReturn(offsetTopicMetadata).when(mockTopics).getPartitionedTopicMetadata(eq(offsetsTopic));

        PulsarAdmin mockPulsarAdmin = mock(PulsarAdmin.class);

        doReturn(mockClusters).when(mockPulsarAdmin).clusters();
        doReturn(mockTenants).when(mockPulsarAdmin).tenants();
        doReturn(mockNamespaces).when(mockPulsarAdmin).namespaces();
        doReturn(mockTopics).when(mockPulsarAdmin).topics();

        MetadataUtils.createKafkaMetadataIfMissing(mockPulsarAdmin, conf);

        verify(mockTenants, times(1)).createTenant(eq(conf.getKafkaMetadataTenant()), any(TenantInfo.class));
        verify(mockNamespaces, times(1)).createNamespace(eq(conf.getKafkaMetadataTenant() + "/"
            + conf.getKafkaMetadataNamespace()), any(Set.class));
        verify(mockNamespaces, times(1)).setNamespaceReplicationClusters(eq(conf.getKafkaMetadataTenant()
            + "/" + conf.getKafkaMetadataNamespace()), any(Set.class));
        verify(mockNamespaces, times(1)).setRetention(eq(conf.getKafkaMetadataTenant() + "/"
            + conf.getKafkaMetadataNamespace()), any(RetentionPolicies.class));
        verify(mockTopics, times(1)).createPartitionedTopic(eq(offsetsTopic), eq(conf.getOffsetsTopicNumPartitions()));
        verify(mockTopics, times(conf.getOffsetsTopicNumPartitions())).createNonPartitionedTopic(any(String.class));

        // Test that cluster is added to existing Tenant if missing
        // Test that the cluster is added to the namespace replication cluster list if it is missing
        // Test that missing offset topic partitions are created

        reset(mockTenants);
        reset(mockNamespaces);
        reset(mockTopics);

        doReturn(Lists.newArrayList("public")).when(mockTenants).getTenants();

        TenantInfo partialTenant = new TenantInfo(Sets.newHashSet(conf.getSuperUserRoles()),
            Sets.newHashSet("other-cluster"));
        doReturn(partialTenant).when(mockTenants).getTenantInfo(eq(conf.getKafkaMetadataTenant()));

        doReturn(Lists.newArrayList("test")).when(mockNamespaces).getNamespaces("public");
        doReturn(emptyList).when(mockNamespaces).getNamespaceReplicationClusters(eq(conf.getKafkaMetadataTenant()));

        List<String> incompleteOffsetPartitionList = new ArrayList<String>(conf.getOffsetsTopicNumPartitions());
        for (int i = 0; i < conf.getOffsetsTopicNumPartitions() - 2; i++) {
            incompleteOffsetPartitionList.add(offsetsTopic + PARTITIONED_TOPIC_SUFFIX + i);
        }
        doReturn(new PartitionedTopicMetadata(8)).when(mockTopics).getPartitionedTopicMetadata(eq(offsetsTopic));
        doReturn(incompleteOffsetPartitionList).when(mockTopics).getList(eq(conf.getKafkaMetadataTenant()
            + "/" + conf.getKafkaMetadataNamespace()));

        MetadataUtils.createKafkaMetadataIfMissing(mockPulsarAdmin, conf);

        verify(mockTenants, times(1)).updateTenant(eq(conf.getKafkaMetadataTenant()), any(TenantInfo.class));
        verify(mockNamespaces, times(1)).setNamespaceReplicationClusters(eq(conf.getKafkaMetadataTenant()
            + "/" + conf.getKafkaMetadataNamespace()), any(Set.class));
        verify(mockTopics, times(2)).createNonPartitionedTopic(any(String.class));
    }
}
