package io.streamnative.pulsar.handlers.kop.utils;

import com.google.api.client.util.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import kafka.zookeeper.ZNodeChangeHandler;
import kafka.zookeeper.ZNodeChildChangeHandler;
import org.apache.zookeeper.KeeperException;
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperClient.AsyncRequest;
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperClient.AsyncResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class KopZkClient {

    private final ZooKeeperClient zooKeeperClient;

    public KopZkClient(ZooKeeperClient zooKeeperClient) {
        this.zooKeeperClient = zooKeeperClient;
    }

    public ZooKeeperClient getZooKeeperClient() {
        return zooKeeperClient;
    }

    public void registerZNodeChildChangeHandler(ZNodeChildChangeHandler zNodeChildChangeHandler) {
        zooKeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler);
    }

    public void unregisterZNodeChildChangeHandler(String path) {
        zooKeeperClient.unregisterZNodeChildChangeHandler(path);
    }

    private boolean registerZNodeChangeHandlerAndCheckExistence(ZNodeChangeHandler zNodeChangeHandler)
            throws InterruptedException, KeeperException {
        zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler);
        AsyncResponse existsResponse = retryRequestUntilConnected(
                new ZooKeeperClient.ExistsRequest(zNodeChangeHandler.path(), Optional.empty()));
        switch (existsResponse.getResultCode()) {
            case OK:
                return true;
            case NONODE:
                return false;
            default:
                throw existsResponse.resultException().get();
        }
    }


    private AsyncResponse retryRequestUntilConnected(AsyncRequest request) throws InterruptedException {
        return retryRequestsUntilConnected(Sets.newHashSet(request)).get(0);
    }

    private List<ZooKeeperClient.AsyncResponse> retryRequestsUntilConnected(Set<AsyncRequest> requests) throws InterruptedException {
        Set<AsyncRequest> remainingRequests = requests;
        ArrayList<AsyncResponse> responses = Lists.newArrayList();
        HashSet<AsyncRequest> remainingRequestsTmp = Sets.newHashSet();
        while (!remainingRequests.isEmpty()) {
            List<AsyncResponse> batchResponses = zooKeeperClient.handleRequests(remainingRequests);

            // Only execute slow path if we find a response with CONNECTIONLOSS
            if (batchResponses.stream()
                    .map(AsyncResponse::getResultCode)
                    .collect(Collectors.toList())
                    .contains(KeeperException.Code.CONNECTIONLOSS)) {
                Streams.zip(
                        remainingRequests.stream(),
                        batchResponses.stream(),
                        (request, response) -> {
                            if (response.getResultCode() == KeeperException.Code.CONNECTIONLOSS) {
                                remainingRequestsTmp.add(request);
                            } else {
                                responses.add(response);
                            }
                            return null;
                        });
                remainingRequests.clear();
                remainingRequests = remainingRequestsTmp;
                if (!remainingRequests.isEmpty()) {
                    zooKeeperClient.waitUntilConnected();
                }
            } else {
                remainingRequests.clear();
                responses.addAll(batchResponses);
            }
        }
        return responses;
    }

    /**
     * Get all topics marked for deletion.
     *
     * @return set of topics marked for deletion.
     */
    public List<String> getTopicDeletions() throws InterruptedException, KeeperException {
        ZooKeeperClient.GetChildrenResponse getChildrenResponse =
                (ZooKeeperClient.GetChildrenResponse) retryRequestUntilConnected(
                        new ZooKeeperClient.GetChildrenRequest(
                                getDeleteTopicsZNodePath(),
                                true,
                                Optional.empty()));

        switch (getChildrenResponse.getResultCode()) {
            case OK:
                return getChildrenResponse.getChildren();
            case NONODE:
                return Collections.emptyList();
            default:
                throw getChildrenResponse.resultException().get();
        }
    }

    public static String getDeleteTopicsZNodePath() {
        return "/kop/delete_topics";
    }

}
