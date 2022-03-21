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
package io.streamnative.pulsar.handlers.kop.coordinator.group;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Unit test of {@link MemberMetadata}.
 */
public class MemberMetadataTest {

    private static final String groupId = "test-group-id";
    private static final String clientId = "test-client-id";
    private static final String clientHost = "test-client-host";
    private static final String memberId = "test-member-id";
    private static final String protocolType = "consumer";
    private static final int rebalanceTimeoutMs = 60000;
    private static final int sessionTimemoutMs = 10000;

    private static MemberMetadata newMember(Map<String, byte[]> protocols) {
        return new MemberMetadata(
            memberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimemoutMs,
            protocolType,
            protocols
        );
    }

    @Test
    public void testMatchesSupportedProtocols() {
        Map<String, byte[]> protocols = Maps.newHashMap();
        protocols.put("range", new byte[0]);

        MemberMetadata member = newMember(protocols);

        assertTrue(member.matches(protocols));

        protocols = new HashMap<>();
        protocols.put("range", new byte[] { 0 });
        assertFalse(member.matches(protocols));

        protocols = new HashMap<>();
        protocols.put("roundrobin", new byte[0]);
        assertFalse(member.matches(protocols));

        protocols = new HashMap<>();
        protocols.put("range", new byte[0]);
        protocols.put("roundrobin", new byte[0]);
        assertFalse(member.matches(protocols));
    }

    @Test
    public void testVoteForPreferredProtocol() {
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("range", new byte[0]);
        protocols.put("roundrobin", new byte[0]);

        MemberMetadata member = newMember(protocols);
        assertEquals(member.vote(Sets.newHashSet("range", "roundrobin")), "range");
        assertEquals(member.vote(Sets.newHashSet("blah", "roundrobin")), "roundrobin");
    }

    @Test
    public void testMetadata() {
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("range", new byte[] { 0xf });
        protocols.put("roundrobin", new byte[] { 0xe });

        MemberMetadata memberMetadata = newMember(protocols);
        assertEquals(
                memberMetadata.metadata("range"),
                new byte[] { 0xf });
        assertEquals(
                memberMetadata.metadata("roundrobin"),
                new byte[] { 0xe });
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMetadataRaisesOnUnsupportedProtocol() {
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("range", new byte[0]);
        protocols.put("roundrobin", new byte[0]);

        MemberMetadata member = newMember(protocols);
        member.metadata("blah");
        fail("Should not reach here");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testVoteRaisesOnUnsupportedProtocols() {
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("range", new byte[0]);
        protocols.put("roundrobin", new byte[0]);

        MemberMetadata member = newMember(protocols);
        member.vote(Sets.newHashSet("blah"));
        fail("Should not reach here");
    }

}
