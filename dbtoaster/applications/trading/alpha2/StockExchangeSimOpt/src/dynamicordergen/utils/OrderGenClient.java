/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dynamicordergen.utils;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

/**
 * A class to create a singleton connection channel to the market on which different threads can send in the simulated orders. One OrderGenClient instance creates one 
 * connection channel to the market which can be used to communicate data by multiple threads.
 * 
 * @author kunal
 */
public class OrderGenClient {
    
    Channel ch;
    public OrderGenClient() {
        ChannelFactory factory =
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                        new StringDecoder(), new StringEncoder());
            }
        });
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        ChannelFuture cf = bootstrap.connect(new InetSocketAddress("localhost", 8080));

        ch = cf.awaitUninterruptibly().getChannel();
    }
    
    public synchronized void write(String str){
        if(ch.isOpen())
         ch.write(str);
    }

    public void close() {
        ch.close();
    }
}
