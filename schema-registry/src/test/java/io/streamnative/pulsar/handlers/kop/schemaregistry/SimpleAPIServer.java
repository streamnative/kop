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
package io.streamnative.pulsar.handlers.kop.schemaregistry;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;

public class SimpleAPIServer {
    private EventLoopGroup group;
    private ServerSocketChannel serverChannel;

    private final SchemaRegistryHandler schemaRegistryHandler;

    public SimpleAPIServer(SchemaRegistryHandler schemaRegistryHandler) {
        this.schemaRegistryHandler = schemaRegistryHandler;
    }

    public void startServer() throws Exception {
        group = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(group, group)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpServerCodec());
                        p.addLast(new HttpObjectAggregator(5 * 1024 * 1024 * 1024));
                        p.addLast(schemaRegistryHandler);
                    }
                });
        ChannelFuture bind = b.bind(0);
        bind.get();
        serverChannel = (ServerSocketChannel) bind.channel();
    }

    public void stopServer() throws Exception {
        if (serverChannel != null) {
            serverChannel.close().get();
        }
        if (group != null) {
            group.shutdownGracefully().await();
            group = null;
        }
    }

    public URL url(String base) throws Exception {
        return new URL("http://localhost:"
                + serverChannel.localAddress().getPort() + base);
    }

    public String executeGet(String base) throws Exception {
        URL url = url(base);
        Object content = url.getContent();
        if (content instanceof InputStream) {
            content = IOUtils.toString((InputStream) content, "utf-8");
        }
        return content.toString();
    }

    public String executePost(String base, String requestContent, String requestContentType) throws Exception {
        return executePost(base, requestContent, requestContentType, null);
    }

    public String executePost(String base, String requestContent,
                              String requestContentType, Integer expectedError) throws Exception {
        URL url = url(base);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        try {
            urlConnection.setDoOutput(true);
            urlConnection.setRequestProperty("Content-Type", requestContentType);
            urlConnection.setRequestProperty("Content-Length", requestContent.length() + "");
            urlConnection.getOutputStream().write(requestContent.getBytes(StandardCharsets.UTF_8));
            urlConnection.getOutputStream().close();
            Object content = urlConnection.getContent();
            if (content instanceof InputStream) {
                content = IOUtils.toString((InputStream) content, "utf-8");
            }
            return content.toString();
        } catch (IOException err) {
            if (expectedError == null) {
                throw err;
            }
            if (urlConnection.getResponseCode() != expectedError.intValue()) {
                throw new IOException("Unexpecter error code "
                        + urlConnection.getResponseCode() + ", expected " + expectedError);
            }
            return IOUtils.toString(urlConnection.getErrorStream(), "utf-8");
        } finally {
            urlConnection.disconnect();
        }
    }

    public String executeDelete(String base) throws Exception {
        URL url = url(base);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        try {
            urlConnection.setRequestMethod("DELETE");
            Object content = urlConnection.getContent();
            if (content instanceof InputStream) {
                content = IOUtils.toString((InputStream) content, "utf-8");
            }
            return content.toString();
        } finally {
            urlConnection.disconnect();
        }
    }
}
