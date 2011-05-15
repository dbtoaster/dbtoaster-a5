/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

/**
 *
 * @author kunal
 */
public class TestClient {

    @Sharable
    public class ClientHandler
            extends SimpleChannelHandler {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            System.out.println("Market update: " + e.getMessage());
        }
    }

    public static void main(String args[]) throws InterruptedException {
        ChannelFactory factory =
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        final TestClient c = new TestClient();
        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                        new StringDecoder(), new StringEncoder(), c.new ClientHandler());
            }
        });
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        ChannelFuture cf = bootstrap.connect(new InetSocketAddress("localhost", 8080));

        Channel ch = cf.awaitUninterruptibly().getChannel();
        writelines(ch);
    }

    private static void writelines(Channel ch) throws InterruptedException {
        try {
            System.out.println("Enter test file name");
            BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
            String filename = b.readLine();
            BufferedReader br = new BufferedReader(new FileReader("./" + filename.trim()));
            int count = 0;
            while (br.ready()) {
                String line = br.readLine();
                count++;
                System.out.println(count + ":" + line);
                ChannelFuture cf = ch.write(line + "\n");
                Thread.currentThread().sleep(1000);
                //cf.awaitUninterruptibly();
                while (!cf.isDone()) {
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
    }
}
