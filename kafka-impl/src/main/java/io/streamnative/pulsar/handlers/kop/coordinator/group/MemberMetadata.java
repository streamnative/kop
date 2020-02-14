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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.kafka.common.protocol.Errors;

/**
 * Member metadata contains the following metadata:
 *
 * <p>Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * <p>Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * <p>In addition, it also contains the following state information:
 *
 * <p>1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
@RequiredArgsConstructor
@NotThreadSafe
@Accessors(fluent = true)
@Getter
@Setter
@SuppressFBWarnings({
    "EI_EXPOSE_REP",
    "EI_EXPOSE_REP2"
})
public class MemberMetadata {

    /**
     * Summary of member metadata.
     */
    @Data
    @SuppressFBWarnings({
        "EI_EXPOSE_REP",
        "EI_EXPOSE_REP2"
    })
    public static class MemberSummary {
        private final String memberId;
        private final String clientId;
        private final String clientHost;
        private final byte[] metadata;
        private final byte[] assignment;
    }

    private final String memberId;
    private final String groupId;
    private final String clientId;
    private final String clientHost;
    private final int rebalanceTimeoutMs;
    private final int sessionTimeoutMs;
    private final String protocolType;
    private final Map<String, byte[]> supportedProtocols;

    private byte[] assignment = new byte[0];
    private CompletableFuture<JoinGroupResult> awaitingJoinCallback = null;
    private BiConsumer<byte[], Errors> awaitingSyncCallback = null;
    private long latestHeartbeat = -1L;
    private boolean isLeaving = false;

    public Set<String> protocols() {
        return supportedProtocols.keySet();
    }

    public void supportedProtocols(Map<String, byte[]> supportedProtocols) {
        this.supportedProtocols.clear();
        this.supportedProtocols.putAll(supportedProtocols);
    }

    public byte[] metadata(String protocol) {
        byte[] metadata = supportedProtocols.get(protocol);
        checkArgument(metadata != null, "Member does not support protocol");
        return metadata;
    }

    /**
     * Check if the provided protocol metadata matches the currently stored metadata.
     */
    public boolean matches(Map<String, byte[]> protocols) {
        if (protocols.size() != this.supportedProtocols.size()) {
            return false;
        }

        for (Map.Entry<String, byte[]> protocolEntry : protocols.entrySet()) {
            byte[] p1 = protocolEntry.getValue();
            byte[] p2 = this.supportedProtocols.get(protocolEntry.getKey());
            if (p2 == null || !Arrays.equals(p1, p2)) {
                return false;
            }
        }
        return true;
    }

    public MemberSummary summary(String protocol) {
        return new MemberSummary(
            memberId,
            clientId,
            clientHost,
            metadata(protocol),
            assignment
        );
    }

    public MemberSummary summaryNoMetadata() {
        return new MemberSummary(
            memberId,
            clientId,
            clientHost,
            new byte[0],
            new byte[0]
        );
    }

    public String vote(Set<String> candidates) {
        Optional<Map.Entry<String, byte[]>> voteProtocol = this.supportedProtocols.entrySet()
            .stream()
            .filter(p -> candidates.contains(p.getKey()))
            .findFirst();
        checkArgument(
            voteProtocol.isPresent(),
            "Member does not support any of the candidate protocols");
        return voteProtocol.get().getKey();
    }

    @Override
    public String toString() {
        ToStringHelper helper = MoreObjects.toStringHelper("MemberMetadata")
            .add("memberId", memberId)
            .add("clientId", clientId)
            .add("clientHost", clientHost)
            .add("sessionTimeoutMs", sessionTimeoutMs)
            .add("rebalanceTimeoutMs", rebalanceTimeoutMs)
            .add("supportedProtocols", protocols().stream());
        return helper.toString();
    }

}
