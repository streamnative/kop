package io.streamnative.pulsar.handlers.kop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaPayloadProcessorTest {

    @Test
    public void test() throws Exception {
        final List<String> values = Arrays.asList("1", "hello", "", null);
        final MockedMessagePayloadContext context = new MockedMessagePayloadContext(
                values.size(),
                new MessageIdImpl(0L, 0L, -1));
        final ByteBufferPayload payload = ByteBufferPayload.newKafkaPayload(values);

        final KafkaPayloadProcessor processor = new KafkaPayloadProcessor();
        final List<String> result = new ArrayList<>();
        processor.process(payload, context, Schema.STRING, message -> result.add(message.getValue()));
        Assert.assertEquals(result, values);
    }
}
