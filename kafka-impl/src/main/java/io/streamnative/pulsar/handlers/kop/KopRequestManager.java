package io.streamnative.pulsar.handlers.kop;

import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Getter
public class KopRequestManager {
    private final KafkaServiceConfiguration kafkaConfig;
    private final Map<Channel, String> channels;
    private final AtomicInteger nextChannelIndex;
    private final List<KopEventManager> requestHandles;

    public KopRequestManager(KafkaServiceConfiguration kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.channels = Maps.newConcurrentMap();
        this.nextChannelIndex = new AtomicInteger(0);
        this.requestHandles = new ArrayList<>(kafkaConfig.getNumRequestThreads());
    }

    private void addRequestEventManager() {
        int maxNumResponseThreads = kafkaConfig.getNumResponseThreads();
        for (int i = 0; i < maxNumResponseThreads; i++) {
            requestHandles.add(new KopEventManager(null,
                    null,
                    null,
                    "kop-request-thread-" + i));
        }
    }

    public void start() {
        addRequestEventManager();
        requestHandles.forEach(KopEventManager::start);
    }

    public void close() {
        requestHandles.forEach(KopEventManager::close);
    }

    public void addRequest(Channel channel,
                           KafkaCommandDecoder.ResponseAndRequest responseAndRequest,
                           KafkaCommandDecoder decoder) {
        if (channels.containsKey(channel)) {
            KopEventManager eventManager = getRequestKopEventManager(channel);
            eventManager.put(eventManager.getKopRequestEvent(responseAndRequest,
                    decoder,
                    kafkaConfig.getRequestTimeoutMs()));
            if (log.isDebugEnabled()) {
                log.debug("AddRequest for request {} to kopEventManager {}, channel {}",
                        responseAndRequest.getRequest().getRequest(), eventManager.getKopEventThreadName(), channel);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("AddResponse for request {} failed, because channel {} not active.",
                        responseAndRequest.getRequest().getRequest(), channel);
            }
        }
    }

    private KopEventManager getRequestKopEventManager(Channel channel) {
        String channelId = channels.get(channel);
        int requestHandleIndex = Math.abs(channelId.hashCode()) % requestHandles.size();
        return requestHandles.get(requestHandleIndex);
    }

    public void setChannel(Channel channel) {
        String channelId = channelId(channel);
        this.channels.put(channel, channelId);
        if (log.isDebugEnabled()) {
            log.debug("set channelId {} for channel {}", channelId, channel);
        }
    }

    public void removeChannel(Channel channel) {
        this.channels.remove(channel);
    }

    protected String channelId(Channel channel) {
        InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
        InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
        String localHost = localAddress.getHostName();
        int localPort = localAddress.getPort();
        String remoteHost = remoteAddress.getHostName();
        int remotePort = remoteAddress.getPort();
        String channelId = localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort + "-" + nextChannelIndex;
        if (nextChannelIndex.get() == Integer.MAX_VALUE) {
            nextChannelIndex.set(0);
        }
        return channelId;
    }


}
