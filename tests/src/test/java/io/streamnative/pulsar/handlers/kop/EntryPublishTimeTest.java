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
package io.streamnative.pulsar.handlers.kop;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Test for entry publish time.
 */
public class EntryPublishTimeTest extends KopProtocolHandlerTestBase {

    KafkaRequestHandler kafkaRequestHandler;
    SocketAddress serviceAddress;

    public EntryPublishTimeTest(String format) {
        super(format);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        kafkaRequestHandler = newRequestHandler();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        kafkaRequestHandler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}