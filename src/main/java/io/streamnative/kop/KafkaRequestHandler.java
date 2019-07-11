package io.streamnative.kop;


import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;

@Slf4j
public class KafkaRequestHandler extends KafkaCommandDecoder {

    private final KafkaService kafkaService;
    private final String clusterName;
    private final String kafkaNamespace;
    private final ExecutorService executor;

    public KafkaRequestHandler(KafkaService kafkaService) {
        super();
        this.kafkaService = kafkaService;

        this.clusterName = kafkaService.getKafkaConfig().getClusterName();
        this.kafkaNamespace = kafkaService.getKafkaConfig().getKafkaNamespace();
        this.executor = kafkaService.getExecutor();
    }

    protected void handleApiVersionsRequest(KafkaHeaderAndRequest apiVersionRequest) {
        AbstractResponse apiResponse = ApiVersionsResponse.defaultApiVersionsResponse();
        ctx.writeAndFlush(responseToByteBuf(apiResponse, apiVersionRequest));
        return;
    }


    protected void handleError(String error) {
        throw new NotImplementedException("handleError");
    }

    protected void handleTopicMetadataRequest(KafkaHeaderAndRequest metadata) {
        throw new NotImplementedException("handleTopicMetadataRequest");
    }

    protected void handleProduceRequest(KafkaHeaderAndRequest produce) {
        throw new NotImplementedException("handleProduceRequest");
    }

    protected void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator) {
        throw new NotImplementedException("handleFindCoordinatorRequest");
    }

    protected void handleListOffsetRequest(KafkaHeaderAndRequest listOffset) {
        throw new NotImplementedException("handleListOffsetRequest");
    }

    protected void handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch) {
        throw new NotImplementedException("handleOffsetFetchRequest");
    }

    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit) {
        throw new NotImplementedException("handleOffsetCommitRequest");
    }

    protected void handleFetchRequest(KafkaHeaderAndRequest fetch) {
        throw new NotImplementedException("handleFetchRequest");
    }

    protected void handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup) {
        throw new NotImplementedException("handleFetchRequest");
    }

    protected void handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup) {
        throw new NotImplementedException("handleSyncGroupRequest");
    }

    protected void handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat) {
        throw new NotImplementedException("handleHeartbeatRequest");
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        ctx.close();
    }

}
